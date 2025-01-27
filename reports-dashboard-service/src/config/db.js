import mysql from "mysql2/promise";
import dotenv from "dotenv";

dotenv.config();

export const dbConnection = () => {
  try {
    const pollConnection = mysql.createPool({
      host: process.env.DB_HOST,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME,
      waitForConnections: true,
    });

    console.log("Ansluten till databasen!");

    return pollConnection;

  } catch (error) {
 console.error("Fel vid anslutning till databasen:", error);
  }
};
