const { MongoClient, GridFSBucket } = require("mongodb");
const { uploadFileToBlob } = require("./blobStorage");
const elastciSearchClient = require("./elasticsearch");
const axios = require("axios");
const processFieldContent =
  require("./mongodbwebhookServies").processFieldContent;
const processBlobField = require("./mongodbwebhookServies").processBlobField;
require("dotenv").config();

// Helper: Convert Stream to Buffer
function streamToBuffer(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (chunk) => chunks.push(chunk));
    stream.on("end", () => resolve(Buffer.concat(chunks)));
    stream.on("error", (err) => reject(err));
  });
}

function splitLargeText(content, maxChunkSize = 30000) {
  const chunks = [];
  for (let i = 0; i < content.length; i += maxChunkSize) {
    chunks.push(content.substring(i, i + maxChunkSize));
  }
  return chunks;
}

exports.mongodbConnectionListener = async () => {
  try {
    console.log("Starting MongoDB Connection Monitor...");

    // Step 1: Get all indices with the prefix "datasource_mongodb_connection_"
    const indicesResponse = await elastciSearchClient.cat.indices({
      format: "json",
    });
    const indices = indicesResponse
      .map((index) => index.index)
      .filter((name) => name.startsWith("datasource_mongodb_connection_"));

    console.log("Found indices: ", indices);

    for (const index of indices) {
      // Step 2: Query ElasticSearch for MongoDB configuration
      const query = {
        query: {
          match_all: {},
        },
      };

      const result = await elastciSearchClient.search({
        index,
        body: query,
      });

      for (const configDoc of result.hits.hits) {
        const {
          mongoUri,
          database,
          collection_name,
          title_field,
          category,
          coid,
        } = configDoc._source;

        console.log(
          `Processing collection: ${collection_name} in database: ${database} at MongoDB URI: ${mongoUri}`
        );

        // Step 3: Connect to MongoDB
        const client = new MongoClient(mongoUri, {
          useNewUrlParser: true,
          useUnifiedTopology: true,
        });

        try {
          await client.connect();
          const db = client.db(database);

          console.log("Database Object: ", db);

          if (!db) {
            throw new Error(`Database "${database}" not found.`);
          }

          const bucket = new GridFSBucket(db, {
            bucketName: collection_name,
          });
          const collection = db.collection(collection_name);

          // Detect whether the collection contains GridFS files or regular documents
          const gridFsFiles = await bucket.find().limit(1).toArray();
          const isGridFs = gridFsFiles.length > 0;

          const data = [];

          if (isGridFs) {
            console.log("Detected GridFS collection. Processing files...");
            const files = await bucket.find().toArray();
            if (files.length > 0) {
              for (const file of files) {
                let processedContent;
                let fileUrl;
                const downloadStream = bucket.openDownloadStream(file._id);
                const buffer = await streamToBuffer(downloadStream);
                const fileName = file[title_field];
                try {
                  // Detect and process MIME Types
                  const { extractedText, mimeType } = await processBlobField(
                    buffer
                  );

                  // Upload to Azure Blob Storage
                  fileUrl = await uploadFileToBlob(buffer, fileName, mimeType);
                  console.log("File URL => ", fileUrl);

                  processedContent = extractedText;
                  console.log(
                    "Extracted text from buffer => ",
                    processedContent
                  );
                } catch (error) {
                  console.error(
                    `Failed to process content for row ID ${file._id}:`,
                    error.message
                  );
                  continue;
                }

                if (processedContent) {
                  const fileSizeInMB = (file.length / (1024 * 1024)).toFixed(2); // Convert size to MB
                  const uploadedAt = file.uploadDate || new Date(); // Fallback to current timestamp if missing

                  const chunks = splitLargeText(processedContent);
                  chunks.forEach((chunk, index) => {
                    data.push({
                      id: `mongodb_${database}_${collection_name}_${file._id}_${index}`,
                      content: chunk,
                      title: fileName,
                      description: "No description",
                      image: null,
                      category: category,
                      fileUrl: fileUrl,
                      fileSize: parseFloat(fileSizeInMB), // Add file size (in MB)
                      uploadedAt: uploadedAt, // Add uploadedAt timestamp
                    });
                  });
                }
              }

              const indexName = `tenant_${coid.toLowerCase()}`;

              const payload = {
                value: data.map((doc) => ({
                  "@search.action": "mergeOrUpload",
                  id: doc.id,
                  title: doc.title,
                  content: doc.content,
                  description: doc.description,
                  image: doc.image,
                  category: doc.category,
                  fileUrl: doc.fileUrl,
                  fileSize: doc.fileSize,
                  uploadedAt: doc.uploadedAt,
                })),
              };

              // Push data to Azure Search
              const esResponse = await axios.post(
                `${process.env.AZURE_SEARCH_ENDPOINT}/indexes/${indexName}/docs/index?api-version=2021-04-30-Preview`,
                payload,
                {
                  headers: {
                    "Content-Type": "application/json",
                    "api-key": process.env.AZURE_SEARCH_API_KEY,
                  },
                }
              );

              console.log("ES Response Data => ", esResponse.data);

              console.log(
                `Files pushed successfully to Azure Search in index: ${indexName}`
              );

              // Update the `updatedAt` field in ElasticSearch for tracking
              await elastciSearchClient.update({
                index,
                id: configDoc._id,
                body: {
                  doc: {
                    updatedAt: new Date().toISOString(),
                  },
                },
              });

              console.log(`Index updated successfully.`);
            } else {
              console.log(
                `No new files detected for collection: ${collection_name}`
              );
            }
          } else {
            console.log(
              "Detected regular documents collection. Processing documents..."
            );
            const documents = await collection.find({}).toArray();
            if (documents.length > 0) {
              for (const document of documents) {
                let processedContent;
                const fileName = document[title_field];
                let fileUrl;
                try {
                  // Process the content dynamically based on its structure
                  if (
                    typeof document === "string" ||
                    typeof document === "number"
                  ) {
                    processedContent = document.toString();
                  } else if (Buffer.isBuffer(document)) {
                    const { extractedText } = await processBlobField(document);
                    processedContent = extractedText;
                  } else if (typeof document === "object") {
                    processedContent = JSON.stringify(document, null, 2);
                  } else {
                    processedContent = document.toString();
                  }

                  // Upload to Azure Blob Storage
                  const buffer = Buffer.from(processedContent, "utf-8");
                  fileUrl = await uploadFileToBlob(
                    buffer,
                    `${fileName || "document"}.txt`,
                    "text/plain"
                  );
                  console.log("File URL => ", fileUrl);
                } catch (error) {
                  console.error(
                    `Failed to process content for row ID ${document._id}:`,
                    error.message
                  );
                  continue;
                }

                if (processedContent) {
                  data.push({
                    id: document._id.toString(),
                    content: processedContent,
                    title: fileName, // Use provided title or fallback
                    description: "No description provided",
                    image: null,
                    category: category,
                    fileUrl: fileUrl,
                    fileSize: null, // Not applicable for non-GridFS documents
                    uploadedAt: document.uploadDate || new Date(), // Use uploadDate or current time
                  });
                }
              }

              const indexName = `tenant_${coid.toLowerCase()}`;

              const payload = {
                value: data.map((doc) => ({
                  "@search.action": "mergeOrUpload",
                  id: doc.id,
                  title: doc.title,
                  content: doc.content,
                  description: doc.description,
                  image: doc.image,
                  category: doc.category,
                  fileUrl: doc.fileUrl,
                  fileSize: doc.fileSize,
                  uploadedAt: doc.uploadedAt,
                })),
              };

              // Push data to Azure Search
              const esResponse = await axios.post(
                `${process.env.AZURE_SEARCH_ENDPOINT}/indexes/${indexName}/docs/index?api-version=2021-04-30-Preview`,
                payload,
                {
                  headers: {
                    "Content-Type": "application/json",
                    "api-key": process.env.AZURE_SEARCH_API_KEY,
                  },
                }
              );

              console.log("ES Response Data => ", esResponse.data);

              console.log(
                `Documents pushed successfully to Azure Search in index: ${indexName}`
              );

              // Update the `updatedAt` field in ElasticSearch for tracking
              await elastciSearchClient.update({
                index,
                id: configDoc._id,
                body: {
                  doc: {
                    updatedAt: new Date().toISOString(),
                  },
                },
              });

              console.log(`Index updated successfully.`);
            } else {
              console.log(
                `No new documents detected for collection: ${collection_name}`
              );
            }
          }
        } finally {
          // Close MongoDB connection
          await client.close();
        }
      }
    }
  } catch (error) {
    console.error("Error in MongoDB Connection Monitor:", error.message);
  }
};
