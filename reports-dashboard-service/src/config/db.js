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
          name !== "life_ratataa_se" && 
          name !== "db_demo84" &&
          name !== "db_p10" &&
          name !== "db_p11" &&
          name !== "db_p5" &&
          name !== "db_t" &&
          name !== "db_d1" &&
          name !== "db_t0407" &&
          name !== "db_testalex"
        )
  
    return { poolConnection, filteredDatabases };
  } catch (error) {
    console.error("Fel vid anslutning till databasen:", error);
    throw error;
  }
};
