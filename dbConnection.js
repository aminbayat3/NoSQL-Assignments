import { MongoClient } from "mongodb";

const uri = "mongodb://localhost:27017";

export const client = new MongoClient(uri, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
});

const dbName = "video";

export const db = client.db(dbName);

export const connectToDatabase = async () => {
  const maxRetries = 2; 
  const delay = 4000; 

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      await client.connect();
      console.log("Connected to MongoDB");
      return; // Exit the function if successful
    } catch (error) {
      console.error(
        `Attempt ${attempt} to connect to MongoDB failed:`,
        error.message
      );
      if (attempt < maxRetries) {
        console.log(`Retrying in ${delay / 1000} seconds...`);
        await new Promise((resolve) => setTimeout(resolve, delay));
      } else {
        console.error("Failed to connect to MongoDB after multiple attempts");
        throw error; 
      }
    }
  }
};

export const closeDatabaseConnection = async () => {
  await client.close();
  console.log("Connection closed");
};
