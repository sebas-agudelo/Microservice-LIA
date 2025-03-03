import mysql from "mysql2/promise";
import dotenv from "dotenv";

dotenv.config();

export const dbConnection = async () => {
  try {
    const poolConnection = mysql.createPool({
      host: process.env.DB_HOST,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      waitForConnections: true,
    });

    console.log("Ansluten till databasen!");

    const [databases] = await poolConnection.query('SHOW DATABASES');
    const filteredDatabases = databases
      .map((db) => db.Database)
      .filter(
        (name) =>
          name !== "db_$DATA" &&
          name !== "cokecce_adoveo_com" &&
          name !== "danskebank_ratataa_se" &&
          name !== "tmp" &&
          name !== "sys" &&
          name !== "mysql" &&
          name !== "life_ratataa_se" &&
          name !== "innodb" &&
          name !== "information_schema" &&
          name !== "performance_schema" &&
          name !== "sas_ratataa_se" &&
          name !== "db_clean_db" &&
          name !== "db_practice4" &&
          name !== "life_ratataa_se" && 
          name !== "db_weeffect"
      )
      // .slice(79, 80) 
      // .slice(32, 33); 
      // .slice(8, 9); 



    return { poolConnection, filteredDatabases };
  } catch (error) {
    console.error("Fel vid anslutning till databasen:", error);
    throw error;
  }
};
