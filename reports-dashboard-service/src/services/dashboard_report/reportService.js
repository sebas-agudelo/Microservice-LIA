import { dbConnection } from "../../config/db.js";

export const reportService = async () => {
  try {

    const { poolConnection, filteredDatabases } = await dbConnection();

    const allResults = await Promise.all(
      filteredDatabases.map(async (db) => {
        

        // await poolConnection.query(`DROP INDEX idx_view ON ${db}.view`);
        // await poolConnection.query(`DROP INDEX idx_participant ON ${db}.participant`);

        // await poolConnection.query(`CREATE INDEX idx_view ON ${db}.view (created)`);
        // await poolConnection.query(`CREATE INDEX idx_participant ON ${db}.participant (created)`);

        console.log(`Procces börjad db: ${db}`);
        const [filtredDateResult] = await poolConnection.query(`
          SELECT DATE_SUB(CURDATE(), INTERVAL 15 DAY) AS filtredDate
          FROM ${db}.view
          UNION
          SELECT DATE_SUB(CURDATE(), INTERVAL 15 DAY) AS filtredDate
          FROM ${db}.participant
        `);
        const filtredDate = filtredDateResult && filtredDateResult[0] ? filtredDateResult[0].filtredDate : null;

        

        const [
          viewRows,
          viewRows2,
          leadsResult,
          paidleadsResult,
          uniqueLeadsResult,
          recuringLeadsResult,
          giftcardsSendResult,
          moneyReceivedResult,
          avaragePaymentResult,
          engagementTimeResult,
          answersPercentageResult,
        ] = await Promise.all([
          poolConnection.query(`
          SELECT campaign_id, 
   CASE 
        WHEN view_r = '' THEN ''        
        WHEN view_r IS NOT NULL THEN TRIM(view_r)  
        ELSE NULL                        
    END AS link, 
    COUNT(*) AS views
FROM ${db}.view
WHERE created >= ?

GROUP BY campaign_id, view_r;

        `,[filtredDate]),
        
          poolConnection.query(`
          SELECT campaign_id, COUNT(*) AS link,
          COUNT(*) AS views
          FROM ${db}.view
          WHERE created >= ?
          GROUP BY campaign_id
        `,[filtredDate]),

          poolConnection.query(`
         SELECT 
            p.campaign_id, 
        CASE 
        WHEN location = '' THEN ''         
        WHEN location IS NOT NULL THEN TRIM(location) 
        ELSE NULL                          
    END AS location, 

             COUNT(CASE 
              WHEN TRIM(COALESCE(p.telephone, p.name, p.email)) THEN 1
              END) AS leads

          FROM ${db}.participant p
          WHERE created >= ?
          -- created >= '2025-02-12' AND created < '2025-02-13'
          GROUP BY p.campaign_id, location

          UNION

            SELECT campaign_id, 'TOTAL' AS location,
           COUNT(CASE 
              WHEN TRIM(COALESCE(p.telephone, p.name, p.email)) THEN 1
              END) AS leads
          FROM ${db}.participant p
          WHERE created >= ?
          GROUP BY p.campaign_id
       
        `,[filtredDate, filtredDate]),
        
        poolConnection.query(`
          SELECT campaign_id, 
                 CASE 
                     WHEN location = '' THEN ''         
                     WHEN location IS NOT NULL THEN TRIM(location) 
                     ELSE NULL                           
                 END AS location, 
                 COUNT(CASE WHEN status = 'PAID' THEN 1 END) AS paid_leads
            FROM ${db}.participant 
            WHERE created >= ?
            GROUP BY campaign_id, location
        
          UNION
        
          SELECT campaign_id, 'TOTAL' AS location,
                 COUNT(CASE WHEN status = 'PAID' THEN 1 END) AS paid_leads
            FROM ${db}.participant 
            WHERE created >= ?
            GROUP BY campaign_id
        `,[filtredDate, filtredDate]),
        

          poolConnection.query(`
                          SELECT campaign_id,
                           CASE 
        WHEN location = '' THEN ''         
        WHEN location IS NOT NULL THEN TRIM(location) 
        ELSE NULL                          
    END AS location, 
                          COUNT(DISTINCT CASE WHEN status = 'PAID' THEN telephone END) AS unique_leads
                          FROM ${db}.participant 
                          WHERE telephone IS NOT NULL
                          AND created >= ?

                                                
                          GROUP BY campaign_id, location

                            UNION
                  
                              SELECT campaign_id, 'TOTAL' AS location,
                              COUNT(DISTINCT CASE WHEN status = 'PAID' THEN telephone END) AS unique_leads
                              FROM ${db}.participant 
                          WHERE telephone IS NOT NULL
                          -- AND created >= DATE_SUB(NOW(), INTERVAL 1 DAY)
                          AND created >= ?
                      
                              GROUP BY campaign_id
                  
                     
                      `,[filtredDate, filtredDate]),

          poolConnection.query(`
                         SELECT campaign_id,
                           CASE 
        WHEN location = '' THEN ''         
        WHEN location IS NOT NULL THEN TRIM(location) 
        ELSE NULL                          
    END AS location, 
                        COUNT(DISTINCT CASE WHEN custom_text4 = 'ACTIVE' THEN telephone END) AS recuring_leads
                        FROM ${db}.participant 
                        WHERE telephone IS NOT NULL
                       AND created >= ?
                  
                        GROUP BY campaign_id, location
                
                        UNION
                  
                              SELECT campaign_id, 'TOTAL' AS location,
                              COUNT(DISTINCT CASE WHEN custom_text4 = 'ACTIVE' THEN telephone END) AS recuring_leads
                             FROM ${db}.participant 
                        WHERE telephone IS NOT NULL
                       AND created >= ?
                             
                              GROUP BY campaign_id
                  
                     
                        `,[filtredDate, filtredDate]),

          poolConnection.query(`
                          SELECT campaign_id, 
                            CASE 
        WHEN location = '' THEN ''         
        WHEN location IS NOT NULL THEN TRIM(location) 
        ELSE NULL                          
    END AS location, 
                          COUNT(*) AS giftcards_sent
                          FROM ${db}.participant 
                          WHERE coupon_send = 1
                          AND created >= ?

                        
                  
                          GROUP BY campaign_id, location 

                             UNION
                  
                              SELECT campaign_id, 'TOTAL' AS location,
                              COUNT(*) AS giftcards_sent
                             FROM ${db}.participant 
                             WHERE coupon_send = 1
                       AND created >= ?
                             
                              GROUP BY campaign_id
                  
                       
                          `,[filtredDate, filtredDate]),

          poolConnection.query(`
                             SELECT campaign_id, 
                            CASE 
        WHEN location = '' THEN ''         
        WHEN location IS NOT NULL THEN TRIM(location) 
        ELSE NULL                          
    END AS location, 

                            SUM(amount) AS money_received

                            FROM ${db}.participant
                            WHERE status = 'PAID'

                            AND created >= ?
                            GROUP BY campaign_id, location
                  
                            UNION 
                  
                            SELECT campaign_id, 'TOTAL' AS location,
    
                           SUM(amount) AS money_received
                            FROM ${db}.participant
     
                            WHERE status = 'PAID'
                    AND created >= ?
                  
                              GROUP BY campaign_id
                  
                       
                        `,[filtredDate, filtredDate]),

          poolConnection.query(`
                            SELECT campaign_id, 
    
                              CASE 
                            WHEN location = '' THEN ''         
        WHEN location IS NOT NULL THEN TRIM(location) 
        ELSE NULL                          
    END AS location, 
                            SUM(CASE WHEN status = 'PAID' THEN amount ELSE 0 END) AS money_received,
                            COUNT(CASE WHEN status = 'PAID' THEN 1 END) AS paid_leads,
                            COALESCE(SUM(CASE WHEN status = 'PAID' THEN amount ELSE 0 END) / NULLIF(COUNT(CASE WHEN status = 'PAID' THEN 1 END), 0), 0) AS avarage_payment

                            FROM ${db}.participant
                            WHERE created >= ?
                            -- created >= '2025-02-12' AND created < '2025-02-13'
                            GROUP BY campaign_id, location
   UNION 
                  
                            SELECT campaign_id, 'TOTAL' AS location,

 SUM(CASE WHEN status = 'PAID' THEN amount ELSE 0 END) AS money_received,
                            COUNT(CASE WHEN status = 'PAID' THEN 1 END) AS paid_leads,
                            COALESCE(SUM(CASE WHEN status = 'PAID' THEN amount ELSE 0 END) / NULLIF(COUNT(CASE WHEN status = 'PAID' THEN 1 END), 0), 0) AS avarage_payment

                            FROM ${db}.participant
                            WHERE created >= ?

                            GROUP BY campaign_id
                          `,[filtredDate, filtredDate]),

          poolConnection.query(`
                            SELECT campaign_id,
                              CASE 
        WHEN location = '' THEN ''         
        WHEN location IS NOT NULL THEN TRIM(location) 
        ELSE NULL                          
    END AS location, 
                            SUM(time_spent) AS engagement_time 
                            FROM ${db}.participant 
                            WHERE created >= ?

                            GROUP BY campaign_id, location 

                             UNION 
                  
                            SELECT campaign_id, 'TOTAL' AS location,
                           SUM(time_spent) AS engagement_time 

                            FROM ${db}.participant
                            WHERE created >= ?

                            GROUP BY campaign_id
                  
                          `,[filtredDate, filtredDate]),

          poolConnection.query(`
                            SELECT campaign_id,
                             CASE 
        WHEN location = '' THEN ''         
        WHEN location IS NOT NULL THEN TRIM(location) 
        ELSE NULL                          
    END AS location, 
                            COUNT(CASE WHEN status = 'ERROR' OR status = 'DECLINED' THEN 1 END) AS answers_percentage
                            FROM ${db}.participant 
                            WHERE created >= ?


                    
                            GROUP BY campaign_id, location
                              UNION 
                  
                            SELECT campaign_id, 'TOTAL' AS location,
                             COUNT(CASE WHEN status = 'ERROR' OR status = 'DECLINED' THEN 1 END) AS answers_percentage
                            FROM ${db}.participant 
                            WHERE created >= ?


                    
                            GROUP BY campaign_id
                         
                          `,[filtredDate, filtredDate]),
        ]);

        console.log(`Process utförd db: ${db}`);

        return {
          db,
          filtredDate,
          viewRows: viewRows[0],
          viewRows2: viewRows2[0],
          leadsResult: leadsResult[0],
          paidleadsResult: paidleadsResult[0],
          uniqueLeadsResult: uniqueLeadsResult[0],
          recuringLeadsResult: recuringLeadsResult[0],
          giftcardsSendResult: giftcardsSendResult[0],
          moneyReceivedResult: moneyReceivedResult[0],
          avaragePaymentResult: avaragePaymentResult[0],
          engagementTimeResult: engagementTimeResult[0],
          answersPercentageResult: answersPercentageResult[0],
        };
      })
    );

    return allResults;
  } catch (error) {
    console.error("Error under databasoperation:", error);
    throw error;
  }
};
