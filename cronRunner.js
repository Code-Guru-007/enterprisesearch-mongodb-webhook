const cron = require("node-cron");
const mongodbConnectionListener = require("./mongodbConnectionListener").mongodbConnectionListener;

// Schedule the cron job to run every 5 minutes
cron.schedule("*/5 * * * *", async () => {
    try {
        console.log("Executing MongoDB Connection Listener job...");
        await mongodbConnectionListener();
        console.log("MongoDB Connection Listener job executed successfully.");
    } catch (error) {
        console.error("Error executing MongoDB Connection Listener job:", error.message);
    }
});

console.log("MongoDB Connection Listener is running...");
