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
          field_name,
          field_type,
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

          if (field_type === "blob") {
            const bucket = new GridFSBucket(database, {
              bucketName: collection_name,
            });
            console.log(
              `Fetching new files from collection: ${collection_name}...`
            );
            const files = await bucket.find().toArray();

            if (files.length > 0) {
              console.log("New Files detected:");

              const data = [];

              for (const file of files) {
                let processedContent;
                let fileUrl;

                try {
                  const downloadStream = bucket.openDownloadStream(file._id);
                  const buffer = await streamToBuffer(downloadStream);
                  const fileName = `mongodb_${database}_${collection_name}_file_${file._id}`;

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
                  const chunks = splitLargeText(processedContent);
                  chunks.forEach((chunk, index) => {
                    data.push({
                      id: `mongodb_${database}_${collection_name}_${file._id}_${index}`,
                      content: chunk,
                      title: config.title || `MongoDB Row ID ${file._id}`,
                      description: config.description || "No description",
                      image: config.image || null,
                      category: category,
                      fileUrl: fileUrl,
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
            const collection = db.collection(collection_name);
            console.log(
              `Fetching new documents from collection: ${collection_name}...`
            );

            // Step 4: Fetch all documents (or implement a custom filter if needed)
            const newDocuments = await collection.find({}).toArray();

            if (newDocuments.length > 0) {
              console.log(`New documents detected:`, newDocuments);

              const data = [];

              for (const document of newDocuments) {
                let processedContent;

                try {
                  // Process the content based on field type
                  processedContent = await processFieldContent(
                    document[field_name],
                    field_type
                  );
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
                    title: `MongoDB Row ID ${document._id}`, // Use provided title or fallback
                    description: "No description provided",
                    image: null,
                    category: category,
                    fileUrl: "",
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
