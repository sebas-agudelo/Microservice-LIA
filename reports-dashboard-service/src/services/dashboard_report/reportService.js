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
          SELECT DATE_SUB(CURDATE(), INTERVAL 18 DAY) AS filtredDate
          FROM ${db}.view
          UNION 
          SELECT DATE_SUB(CURDATE(), INTERVAL 18 DAY) AS filtredDate
          FROM ${db}.participant
        `);
        const filtredDate =
          filtredDateResult && filtredDateResult[0]
            ? filtredDateResult[0].filtredDate
            : null;

        const [
          viewRows,
          viewResult,
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
          poolConnection.query(
            `
          SELECT campaign_id,    
           GROUP_CONCAT(view_key) AS view_keys,  
           CASE 
               WHEN view_r = '' THEN ''        
               WHEN view_r IS NOT NULL THEN TRIM(view_r)  
               ELSE NULL                        
           END AS link, 
           COUNT(*) AS views
    FROM ${db}.view
    WHERE created >= ?
    GROUP BY campaign_id, view_r;

          `,
            [filtredDate]
          ),

          poolConnection.query(
            `
            SELECT campaign_id, COUNT(*) AS link,
            COUNT(*) AS views
  
            FROM ${db}.view
            WHERE created >= ?
  
            GROUP BY campaign_id
  
            `,
            [filtredDate]
          ),

          //Leads
          poolConnection.query(
            `
     SELECT 
    p.campaign_id,
    v.view_r AS link,
    COUNT(*) AS leads
FROM ${db}.participant p
JOIN ${db}.view v ON p.view_key = v.view_key
WHERE p.created >= ?
AND p.view_key IS NOT NULL AND p.view_key <> ''
AND (TRIM(NULLIF(p.telephone, '')) <> '' 
    OR TRIM(NULLIF(p.name, '')) <> '' 
    OR TRIM(NULLIF(p.email, '')) <> '')
GROUP BY p.campaign_id, link

UNION

   SELECT 
    p.campaign_id, 
    NULL AS link,  -- Ingen länk här för andra frågan
    COUNT(CASE 
          WHEN TRIM(COALESCE(p.telephone, p.name, p.email)) <> '' THEN 1
          END) AS leads
FROM ${db}.participant p
WHERE p.created >= ?
GROUP BY p.campaign_id;




          `,
            [filtredDate, filtredDate]
          ),

          //paid_leads
          poolConnection.query(
            `
          SELECT p.campaign_id, 
       v.view_r AS link,
       COUNT(CASE WHEN status = 'PAID' THEN 1 END) AS paid_leads
       FROM ${db}.participant p
       JOIN ${db}.view v ON p.view_key = v.view_key
       WHERE p.created >= ?
       AND p.view_key IS NOT NULL AND p.view_key <> ''
       GROUP BY p.campaign_id, link

       UNION

   SELECT 
    p.campaign_id, 
    NULL AS link, 

          COUNT(CASE WHEN status = 'PAID' THEN 1 END) AS paid_leads
       FROM ${db}.participant p
       WHERE p.created >= ?
       GROUP BY p.campaign_id
       
       `,
            [filtredDate, filtredDate]
          ),

          //unique_leads
          poolConnection.query(
            `
                      SELECT p.campaign_id, 
                      v.view_r AS link,
                      COUNT(DISTINCT CASE WHEN status = 'PAID' THEN telephone END) AS unique_leads
                      FROM ${db}.participant p
                      JOIN ${db}.view v ON p.view_key = v.view_key
                      WHERE p.telephone IS NOT NULL
                      AND p.created >= ?
                      AND p.view_key IS NOT NULL AND p.view_key <> ''
                      GROUP BY p.campaign_id, link
                                            

                      UNION
              
                      SELECT 
                      p.campaign_id, 
                      NULL AS link, 
                      COUNT(DISTINCT CASE WHEN status = 'PAID' THEN telephone END) AS unique_leads
                      FROM ${db}.participant p
                      WHERE p.telephone IS NOT NULL
                      AND p.created >= ?
                    
                      GROUP BY p.campaign_id
                 
       `,
            [filtredDate, filtredDate]
          ),

          //recuring_leads
          poolConnection.query(
            `
                    SELECT p.campaign_id, 
                    v.view_r AS link,
                    COUNT(DISTINCT CASE WHEN custom_text4 = 'ACTIVE' THEN telephone END) AS recuring_leads
                   FROM ${db}.participant p
                    JOIN ${db}.view v ON p.view_key = v.view_key
                    WHERE p.telephone IS NOT NULL
                   AND p.created >= ?
              
                          GROUP BY p.campaign_id, link
            
                    UNION
                        
                     SELECT p.campaign_id, 
                 NULL AS link, 
                          COUNT(DISTINCT CASE WHEN custom_text4 = 'ACTIVE' THEN telephone END) AS recuring_leads
                         FROM ${db}.participant p
                    WHERE p.telephone IS NOT NULL
                   AND p.created >= ?
                         
                           GROUP BY p.campaign_id
              
                 
                    `,
            [filtredDate, filtredDate]
          ),

          //Giftcards_sent
          poolConnection.query(
            `
                       SELECT p.campaign_id, 
                    v.view_r AS link,
                      COUNT(*) AS giftcards_sent
                      FROM ${db}.participant p
                    JOIN ${db}.view v ON p.view_key = v.view_key
                      WHERE p.coupon_send = 1
                   AND p.created >= ?
              
                          GROUP BY p.campaign_id, link

                         UNION
              
                                
                     SELECT p.campaign_id, 
                 NULL AS link, 
                          COUNT(*) AS giftcards_sent
                         FROM ${db}.participant p
                         WHERE p.coupon_send = 1
                   AND p.created >= ?
                         
                          GROUP BY p.campaign_id
              
                   
                      `,
            [filtredDate, filtredDate]
          ),

          //money_received
          poolConnection.query(
            `
                      SELECT p.campaign_id, 
                    v.view_r AS link,
                        SUM(amount) AS money_received

                        FROM ${db}.participant p
                    JOIN ${db}.view v ON p.view_key = v.view_key
                        WHERE p.status = 'PAID'

                        AND p.created >= ?
                        GROUP BY p.campaign_id, link
              
                        UNION 
              
                                    
                     SELECT p.campaign_id, 
                 NULL AS link, 

                       SUM(amount) AS money_received
                        FROM ${db}.participant p
 
                        WHERE p.status = 'PAID'
                AND p.created >= ?
              
                          GROUP BY p.campaign_id
              
                   
                    `,
            [filtredDate, filtredDate]
          ),

          //avarage_payment
          poolConnection.query(
            `
                      SELECT p.campaign_id, 
                    v.view_r AS link,
                        SUM(CASE WHEN status = 'PAID' THEN amount ELSE 0 END) AS money_received,
                        COUNT(CASE WHEN status = 'PAID' THEN 1 END) AS paid_leads,
                        COALESCE(SUM(CASE WHEN status = 'PAID' THEN amount ELSE 0 END) / NULLIF(COUNT(CASE WHEN status = 'PAID' THEN 1 END), 0), 0) AS avarage_payment

                        FROM ${db}.participant p
                    JOIN ${db}.view v ON p.view_key = v.view_key
                        WHERE p.created >= ?
                 
                        GROUP BY p.campaign_id, link
UNION 
              
                        SELECT p.campaign_id, 
                 NULL AS link, 

SUM(CASE WHEN status = 'PAID' THEN amount ELSE 0 END) AS money_received,
                        COUNT(CASE WHEN status = 'PAID' THEN 1 END) AS paid_leads,
                        COALESCE(SUM(CASE WHEN status = 'PAID' THEN amount ELSE 0 END) / NULLIF(COUNT(CASE WHEN status = 'PAID' THEN 1 END), 0), 0) AS avarage_payment
     FROM ${db}.participant p
                        WHERE p.created >= ?

                        GROUP BY p.campaign_id
                      `,
            [filtredDate, filtredDate]
          ),

          //engagement_time
          poolConnection.query(
            `
                    SELECT p.campaign_id, 
                    v.view_r AS link,
                        SUM(time_spent) AS engagement_time 
                      
                        FROM ${db}.participant p
                    JOIN ${db}.view v ON p.view_key = v.view_key
                        WHERE p.created >= ?

                        GROUP BY p.campaign_id, link 

                         UNION 
              
                         SELECT p.campaign_id, 
                 NULL AS link, 
                       SUM(time_spent) AS engagement_time 

                        FROM ${db}.participant p
                        WHERE p.created >= ?

                        GROUP BY p.campaign_id
              
                      `,
            [filtredDate, filtredDate]
          ),

          //answers_percentage
          poolConnection.query(
            `
                       SELECT 
 p.campaign_id,
 v.view_r AS link,
 COUNT(*) AS answers_percentage
FROM ${db}.participant p
JOIN ${db}.view v ON p.view_key = v.view_key
WHERE p.created >= ?
AND p.view_key IS NOT NULL AND p.view_key <> ''
AND (p.status = 'ERROR' OR p.status = 'DECLINED')
GROUP BY p.campaign_id, link

UNION 

                  SELECT 
 p.campaign_id,
   NULL AS link, 
 COUNT(*) AS answers_percentage
FROM ${db}.participant p
WHERE p.created >= ?
AND (p.status = 'ERROR' OR p.status = 'DECLINED')
GROUP BY p.campaign_id, link
                      
                       `,
            [filtredDate, filtredDate]
          ),
        ]);

        console.log(`Process utförd db: ${db}`);

        return {
          db,
          filtredDate,
          viewRows: viewRows[0],
          viewResult: viewResult[0],
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
