import { dbConnection } from "../../config/db.js";

export const collectDataForLeads = async () => {
    try{
        const { poolConnection, filteredDatabases } = await dbConnection();

        const leadsResult = await Promise.all(
            filteredDatabases.map(async (db) => {
                console.log("Databasen som är vald", db);
                
                const [
                    locationsResult
                ] = await Promise.all([
        
                    poolConnection.query(`
                    SELECT GROUP_CONCAT(location SEPARATOR ',') AS locations
                    FROM ${db}.participant
                    WHERE location IS NOT NULL AND locations <> ''
                    AND created >= '2024-10-03' AND created < '2024-10-04'
                    GROUP BY telephone
        
                    `)
        
                ])
                console.log(locationsResult);
            })
        )

        return leadsResult;


        

    }catch(error){
        console.error("Error under databasoperation:", error);
        
    }
};

