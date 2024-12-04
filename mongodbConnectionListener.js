const { MongoClient } = require('mongodb');
const client = require("./elasticsearch");
const axios = require("axios");
require("dotenv").config();

exports.mongodbConnectionListener = async () => {
    try {
        console.log("Starting MongoDB Connection Monitor...");

        // Step 1: Get all indices with the prefix "datasource_mongodb_connection_"
        const indicesResponse = await client.cat.indices({ format: "json" });
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

            const result = await client.search({
                index,
                body: query,
            });

            for (const configDoc of result.hits.hits) {
                const {
                    mongoUri,
                    database,
                    collection_name,
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
                    const collection = db.collection(collection_name);

                    console.log(`Fetching new documents from collection: ${collection_name}...`);

                    // Step 4: Fetch all documents (or implement a custom filter if needed)
                    const newDocuments = await collection.find({}).toArray();

                    if (newDocuments.length > 0) {
                        console.log(`New documents detected:`, newDocuments);

                        // Map documents to the required format
                        const data = newDocuments.map((doc) => ({
                            id: doc._id.toString(),
                            title: doc.title,
                            content: doc.content,
                            description: doc.description,
                            image: doc.image,
                            category: category,
                        }));

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
                        await client.update({
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
                        console.log(`No new documents detected for collection: ${collection_name}`);
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
