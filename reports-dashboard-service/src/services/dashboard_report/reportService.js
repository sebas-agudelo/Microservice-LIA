import { dbConnection } from "../../config/db.js";

export const reportService = async () => {
  try {
    const { poolConnection, filteredDatabases } = await dbConnection();

    const allResults = await Promise.all(
      filteredDatabases.map(async (db) => {

        //Checks if the idx_view index already exists.
        const [existingViewIndex] = await poolConnection.query(`
        SHOW INDEXES FROM ${db}.view WHERE Key_name = 'idx_view';
        `);
        
        //Checks if the idx_participant index already exists.
        const [existingParticipantIndex] = await poolConnection.query(`
        SHOW INDEXES FROM ${db}.participant WHERE Key_name = 'idx_participant';
        `);

        //If idx_view does not exist, it will be created.
        if (!existingViewIndex.length) {
          await poolConnection.query(`
            CREATE INDEX idx_view ON ${db}.view (created)`
          );
        }

        //If idx_participant does not exist, it will be created.
        if (!existingParticipantIndex.length) {
          await poolConnection.query(`
            CREATE INDEX idx_participant ON ${db}.participant (created)`
          );
        }

        console.log(`Procces börjad db: ${db}`);
        const [filtredDateResult] = await poolConnection.query(`
          SELECT DATE_SUB(CURDATE(), INTERVAL 1 DAY) AS filtredDate
          FROM ${db}.view
          UNION 
          SELECT DATE_SUB(CURDATE(), INTERVAL 1 DAY) AS filtredDate
          FROM ${db}.participant
        `);
        const filtredDate =
          filtredDateResult && filtredDateResult[0]
            ? filtredDateResult[0].filtredDate
            : null;

        const [
          viewLinkResult,
          viewCampaignResult,
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

          //The query to retrieve data based on campaign_id and view_r (just for the links)
          poolConnection.query(`
          SELECT campaign_id, view_key,  
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

          //The query to retrieve data based on campaign_id (just for the campaign)
          poolConnection.query(`
          SELECT campaign_id, 
            
          COUNT(*) AS link,
          COUNT(*) AS views
  
          FROM ${db}.view
          WHERE created >= ?
          GROUP BY campaign_id
          `,[filtredDate]),

          //Leads fetch data from the participant and check if the view_key matches the one in the view table.
          poolConnection.query(`
          SELECT p.campaign_id, v.view_r AS link,

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

          SELECT p.campaign_id, 
          NULL AS link, 
          
          COUNT(CASE 
              WHEN TRIM(COALESCE(p.telephone, p.name, p.email)) <> '' THEN 1
          END) AS leads
          
          FROM ${db}.participant p
          WHERE p.created >= ?
          GROUP BY p.campaign_id
          `,[filtredDate, filtredDate]),

          //paid_leads fetch data from the participant and check if the view_key matches the one in the view table.
          poolConnection.query(`
          SELECT p.campaign_id, v.view_r AS link,

          COUNT(CASE WHEN status = 'PAID' THEN 1 END) AS paid_leads
          
          FROM ${db}.participant p
          JOIN ${db}.view v ON p.view_key = v.view_key
          WHERE p.created >= ?
          AND p.view_key IS NOT NULL AND p.view_key <> ''
          GROUP BY p.campaign_id, link

          UNION

          SELECT p.campaign_id, NULL AS link, 

          COUNT(CASE WHEN status = 'PAID' THEN 1 END) AS paid_leads

          FROM ${db}.participant p
          WHERE p.created >= ?
          GROUP BY p.campaign_id
          `,[filtredDate, filtredDate]),

          //unique_leads fetch data from the participant and check if the view_key matches the one in the view table.
          poolConnection.query(`
          SELECT p.campaign_id, v.view_r AS link,

          COUNT(DISTINCT CASE WHEN status = 'PAID' THEN telephone END) AS unique_leads

          FROM ${db}.participant p
          JOIN ${db}.view v ON p.view_key = v.view_key
          WHERE p.telephone IS NOT NULL
          AND p.created >= ?
          AND p.view_key IS NOT NULL AND p.view_key <> ''
          GROUP BY p.campaign_id, link
                                            
          UNION
              
          SELECT p.campaign_id, NULL AS link, 

          COUNT(DISTINCT CASE WHEN status = 'PAID' THEN telephone END) AS unique_leads
            
          FROM ${db}.participant p
          WHERE p.telephone IS NOT NULL
          AND p.created >= ?
          GROUP BY p.campaign_id
          `,[filtredDate, filtredDate]),

          //recuring_leads fetch data from the participant and check if the view_key matches the one in the view table.
          poolConnection.query(`
          SELECT p.campaign_id, v.view_r AS link,
          COUNT(DISTINCT CASE WHEN custom_text4 = 'ACTIVE' THEN telephone END) AS recuring_leads
          
          FROM ${db}.participant p
          JOIN ${db}.view v ON p.view_key = v.view_key
          WHERE p.telephone IS NOT NULL
          AND p.created >= ?
          GROUP BY p.campaign_id, link
            
          UNION
                        
          SELECT p.campaign_id, NULL AS link, 

          COUNT(DISTINCT CASE WHEN custom_text4 = 'ACTIVE' THEN telephone END) AS recuring_leads

          FROM ${db}.participant p
          WHERE p.telephone IS NOT NULL
          AND p.created >= ?
          GROUP BY p.campaign_id
          `,[filtredDate, filtredDate]),

          //Giftcards_sent fetch data from the participant and check if the view_key matches the one in the view table.
          poolConnection.query(`
          SELECT p.campaign_id, v.view_r AS link,
          
          COUNT(*) AS giftcards_sent
                      
          FROM ${db}.participant p
          JOIN ${db}.view v ON p.view_key = v.view_key
          WHERE p.coupon_send = 1
          AND p.created >= ?
          GROUP BY p.campaign_id, link

          UNION
              
          SELECT p.campaign_id, NULL AS link, 

          COUNT(*) AS giftcards_sent

          FROM ${db}.participant p
          WHERE p.coupon_send = 1
          AND p.created >= ?
          GROUP BY p.campaign_id
          `,[filtredDate, filtredDate]),

          //money_received fetch data from the participant and check if the view_key matches the one in the view table.
          poolConnection.query(`
          SELECT p.campaign_id, v.view_r AS link,
        
          SUM(amount) AS money_received
          
          FROM ${db}.participant p
          JOIN ${db}.view v ON p.view_key = v.view_key
          WHERE p.status = 'PAID'
          AND p.created >= ?
          GROUP BY p.campaign_id, link
              
          UNION 
              
          SELECT p.campaign_id, NULL AS link, 

          SUM(amount) AS money_received

          FROM ${db}.participant p
          WHERE p.status = 'PAID'
          AND p.created >= ?
          GROUP BY p.campaign_id
          `,[filtredDate, filtredDate]),

          //avarage_payment fetch data from the participant and check if the view_key matches the one in the view table.
          poolConnection.query(`
          SELECT p.campaign_id, v.view_r AS link,
              
          SUM(CASE WHEN status = 'PAID' THEN amount ELSE 0 END) AS money_received,
          COUNT(CASE WHEN status = 'PAID' THEN 1 END) AS paid_leads,
          COALESCE(SUM(CASE WHEN status = 'PAID' THEN amount ELSE 0 END) / NULLIF(COUNT(CASE WHEN status = 'PAID' THEN 1 END), 0), 0) AS avarage_payment

          FROM ${db}.participant p
          JOIN ${db}.view v ON p.view_key = v.view_key
          WHERE p.created >= ?
          GROUP BY p.campaign_id, link

          UNION 
          SELECT p.campaign_id, NULL AS link, 

          SUM(CASE WHEN status = 'PAID' THEN amount ELSE 0 END) AS money_received,
          COUNT(CASE WHEN status = 'PAID' THEN 1 END) AS paid_leads,
          COALESCE(SUM(CASE WHEN status = 'PAID' THEN amount ELSE 0 END) / NULLIF(COUNT(CASE WHEN status = 'PAID' THEN 1 END), 0), 0) AS avarage_payment
     
          FROM ${db}.participant p
          WHERE p.created >= ?
          GROUP BY p.campaign_id
          `,[filtredDate, filtredDate]),

          //engagement_time fetch data from the participant and check if the view_key matches the one in the view table.
          poolConnection.query(`
          SELECT p.campaign_id, v.view_r AS link,
            
          SUM(time_spent) AS engagement_time 
                      
          FROM ${db}.participant p
          JOIN ${db}.view v ON p.view_key = v.view_key
          WHERE p.created >= ?
          GROUP BY p.campaign_id, link 

          UNION 
              
          SELECT p.campaign_id, NULL AS link, 
        
          SUM(time_spent) AS engagement_time 
          FROM ${db}.participant p
          WHERE p.created >= ?
          GROUP BY p.campaign_id
          `,[filtredDate, filtredDate]),

          //answers_percentage fetch data from the participant and check if the view_key matches the one in the view table.
          poolConnection.query(`
          SELECT p.campaign_id, v.view_r AS link,
 
          COUNT(*) AS answers_percentage
          FROM ${db}.participant p
          JOIN ${db}.view v ON p.view_key = v.view_key
          WHERE p.created >= ?

          AND p.view_key IS NOT NULL AND p.view_key <> ''
          AND (p.status = 'ERROR' OR p.status = 'DECLINED')
          GROUP BY p.campaign_id, link

          UNION 

          SELECT p.campaign_id, NULL AS link, 
          COUNT(*) AS answers_percentage

          FROM ${db}.participant p
          WHERE p.created >= ?

          AND (p.status = 'ERROR' OR p.status = 'DECLINED')
          GROUP BY p.campaign_id, link
          `,[filtredDate, filtredDate]),
        ]);

        console.log(`Process utförd db: ${db}`);

        return {
          db,
          filtredDate,
          viewLinkResult: viewLinkResult[0],
          viewCampaignResult: viewCampaignResult[0],
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
