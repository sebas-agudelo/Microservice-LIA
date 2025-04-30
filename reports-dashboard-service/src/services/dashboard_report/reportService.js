import { dbConnection } from "../../config/db.js";

export const reportService = async () => {
  try {
    const { poolConnection, filteredDatabases } = await dbConnection();

    const results = [];

    for (const db of filteredDatabases) {
      try {
        // Kontrollera och skapa index om de saknas
        const [existingViewIndex] = await poolConnection.query(`
          SHOW INDEXES FROM ${db}.view WHERE Key_name = 'idx_view';
        `);
        if (!existingViewIndex.length) {
          await poolConnection.query(
            `CREATE INDEX idx_view ON ${db}.view (created)`
          );
        }

        const [existingParticipantIndex] = await poolConnection.query(`
          SHOW INDEXES FROM ${db}.participant WHERE Key_name = 'idx_participant';
        `);
        if (!existingParticipantIndex.length) {
          await poolConnection.query(
            `CREATE INDEX idx_participant ON ${db}.participant (created)`
          );
        }

        console.log(`Startar process för databas: ${db}`);

        const [filtredDateResult] = await poolConnection.query(`
          SELECT '2025-04-14' AS startDate, '2025-04-16' AS endDate
          FROM ${db}.view
          UNION 
          SELECT '2025-04-14' AS startDate, '2025-04-16' AS endDate
          FROM ${db}.participant
        `);

          // Om inga resultat finns, hoppa över databasen
          if (!filtredDateResult.length) {
            console.log(`Ingen data i view eller participant för ${db}, hoppar över.`);
            continue;
          }

        const startDate = filtredDateResult[0].startDate;
        const endDate = filtredDateResult[0].endDate;

        console.log("Start date",startDate);
        console.log("End date",endDate);

        //The query to retrieve data based on campaign_id and view_r (just for the links)
        const [viewLinkResult] = await poolConnection.query(
          `
SELECT 
    v.campaign_id,
    CASE 
        WHEN v.view_r = 'null' AND v.banner_id IS NULL THEN 'null'
        WHEN v.view_r = 'null' AND v.banner_id IS NOT NULL AND b.title IS NOT NULL THEN b.title  
        WHEN v.view_r IS NOT NULL AND v.view_r <> '' AND v.view_r <> 'null' THEN TRIM(v.view_r)  
        WHEN v.view_r IS NULL OR v.view_r = '' OR v.view_r = 'null' THEN 
            CASE 
                WHEN b.title IS NOT NULL THEN b.title 
                ELSE NULL 
            END
        ELSE NULL
    END AS link,
    COUNT(*) AS views
FROM 
    db_practice.view v
LEFT JOIN 
    db_practice.banner b ON b.id = v.banner_id
WHERE 
    (v.view_r IS NOT NULL OR v.banner_id IS NOT NULL)
  AND DATE(v.created) BETWEEN ? AND ?
GROUP BY 
    v.campaign_id,
    link



       
          `,
          [startDate, endDate]
        );        

        //The query to retrieve data based on campaign_id (just for the campaign)
        const [viewCampaignResult] = await poolConnection.query(
          `
          SELECT campaign_id, 
            
          COUNT(*) AS link,
          COUNT(*) AS views
  
          FROM ${db}.view
          WHERE DATE(created) BETWEEN ? AND ?
          GROUP BY campaign_id
          `,
          [startDate, endDate]
        );

      // Leads fetch data from the participant and check if the view_key matches the one in the view table.
      const [leadsResult] = await poolConnection.query(
          `
      SELECT 
      p.campaign_id,
      CASE
      WHEN v.view_r = 'null' AND v.banner_id IS NOT NULL THEN b.title  
      ELSE v.view_r 
      END AS link,
      COUNT(*) AS leads
      FROM 
      ${db}.participant p
      JOIN 
      ${db}.view v ON p.view_key = v.view_key
      LEFT JOIN 
      ${db}.banner b  ON v.banner_id = b.id
      WHERE DATE(p.created) BETWEEN ? AND ?
      AND p.view_key IS NOT NULL AND p.view_key <> ''
      AND (
      TRIM(NULLIF(p.telephone, '')) <> '' 
      OR TRIM(NULLIF(p.name, '')) <> '' 
      OR TRIM(NULLIF(p.email, '')) <> ''
      )
      AND NOT (
      (v.view_r IS NULL OR TRIM(v.view_r) = '')  -- fall 3: båda är null/tomma
      AND v.banner_id IS NULL
      )
      GROUP BY 
      p.campaign_id, 
      link


      UNION
          
      SELECT p.campaign_id, NULL AS link, 
      COUNT(CASE 
      WHEN TRIM(COALESCE(p.telephone, p.name, p.email)) <> '' THEN 1
      END) AS leads
      FROM ${db}.participant p
      WHERE DATE(p.created) BETWEEN ? AND ?
      GROUP BY p.campaign_id
      `,
      [startDate, endDate, startDate, endDate]
      );

      // //paid_leads fetch data from the participant and check if the view_key matches the one in the view table.
      const [paidleadsResult] = await poolConnection.query(
      `
      SELECT 
      p.campaign_id,
      CASE
      WHEN v.view_r = 'null' AND v.banner_id IS NOT NULL THEN b.title
      ELSE v.view_r
      END AS link,
      COUNT(CASE WHEN p.status = 'PAID' THEN 1 END) AS paid_leads
      FROM 
      ${db}.participant p
      JOIN 
      ${db}.view v ON p.view_key = v.view_key
      LEFT JOIN 
      ${db}.banner b ON v.banner_id = b.id
      WHERE DATE(p.created) BETWEEN ? AND ?
      AND p.view_key IS NOT NULL AND p.view_key <> ''
      AND NOT (
      (v.view_r IS NULL OR TRIM(v.view_r) = '')
      AND v.banner_id IS NULL
      )
      GROUP BY 
      p.campaign_id,
      link
  
      UNION
  
      SELECT p.campaign_id, NULL AS link, 
  
      COUNT(CASE WHEN status = 'PAID' THEN 1 END) AS paid_leads
  
      FROM ${db}.participant p
      WHERE DATE(p.created) BETWEEN ? AND ?
      GROUP BY p.campaign_id
      `,
      [startDate, endDate, startDate, endDate]
      );

      // //unique_leads fetch data from the participant and check if the view_key matches the one in the view table.
      const [uniqueLeadsResult] = await poolConnection.query(
      `
      SELECT 
      p.campaign_id,
      CASE
      WHEN v.view_r = 'null' AND v.banner_id IS NOT NULL THEN b.title
      ELSE v.view_r
      END AS link,
      COUNT(DISTINCT CASE WHEN status = 'PAID' THEN telephone END) AS unique_leads
      FROM 
      ${db}.participant p
      JOIN 
      ${db}.view v ON p.view_key = v.view_key
      LEFT JOIN 
      ${db}.banner b ON v.banner_id = b.id
      WHERE 
      p.telephone IS NOT NULL
      AND DATE(p.created) BETWEEN ? AND ?
      AND p.view_key IS NOT NULL AND p.view_key <> ''
      AND NOT (
      (v.view_r IS NULL OR TRIM(v.view_r) = '')
      AND v.banner_id IS NULL
      )
      GROUP BY 
      p.campaign_id, link

                                              
      UNION
                
      SELECT p.campaign_id, NULL AS link, 
  
      COUNT(DISTINCT CASE WHEN status = 'PAID' THEN telephone END) AS unique_leads
              
      FROM ${db}.participant p
      WHERE p.telephone IS NOT NULL
      AND DATE(p.created) BETWEEN ? AND ?
      GROUP BY p.campaign_id
      `,
      [startDate, endDate, startDate, endDate]
      );

        //recuring_leads fetch data from the participant and check if the view_key matches the one in the view table.
        const [recuringLeadsResult] = await poolConnection.query(
          `
        SELECT 
        p.campaign_id,
        CASE
        WHEN v.view_r = 'null' AND v.banner_id IS NOT NULL THEN b.title
        ELSE v.view_r
        END AS link,
        COUNT(DISTINCT CASE WHEN custom_text4 = 'ACTIVE' THEN telephone END) AS recuring_leads
        FROM 
        ${db}.participant p
        JOIN 
        ${db}.view v ON p.view_key = v.view_key
        LEFT JOIN 
        ${db}.banner b ON v.banner_id = b.id
        WHERE 
        p.telephone IS NOT NULL
        AND DATE(p.created) BETWEEN ? AND ?
        AND NOT (
        (v.view_r IS NULL OR TRIM(v.view_r) = '')
        AND v.banner_id IS NULL
        )
        GROUP BY 
        p.campaign_id, link

        UNION
                          
        SELECT p.campaign_id, NULL AS link, 
  
        COUNT(DISTINCT CASE WHEN custom_text4 = 'ACTIVE' THEN telephone END) AS recuring_leads
  
        FROM ${db}.participant p
        WHERE p.telephone IS NOT NULL
        AND DATE(p.created) BETWEEN ? AND ?
        GROUP BY p.campaign_id
        `,
        [startDate, endDate, startDate, endDate]
        );

        // sms_parts fetch data from the participant and check if the view_key matches the one in the view table.
        const [smsPartsResult] = await poolConnection.query(
          `
        SELECT  
        p.campaign_id, 
        CASE
        WHEN v.view_r = 'null' AND v.banner_id IS NOT NULL THEN b.title
        ELSE v.view_r
        END AS link,
        SUM(
        CASE
            WHEN report_download_email IS NOT NULL THEN (
                SELECT SUM(CAST(JSON_UNQUOTE(JSON_EXTRACT(json_obj, '$.parts')) AS UNSIGNED))
                FROM (
                    SELECT TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(CONCAT(report_download_email, '-'), '-', numbers.n), '-', -1)) AS json_obj
                    FROM (
                        SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
                    ) AS numbers
                    WHERE TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(CONCAT(report_download_email, '-'), '-', numbers.n), '-', -1)) != ''
                      AND JSON_VALID(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(CONCAT(report_download_email, '-'), '-', numbers.n), '-', -1)))
                ) AS json_objects
            )
            ELSE 0
        END
        ) AS sms_parts
        FROM ${db}.participant p
        JOIN ${db}.view v ON p.view_key = v.view_key
        LEFT JOIN ${db}.banner b ON v.banner_id = b.id
        WHERE p.created BETWEEN ? AND ?
        AND NOT (
        (v.view_r IS NULL OR TRIM(v.view_r) = '')
        AND v.banner_id IS NULL
        )
        GROUP BY p.campaign_id, link

  
            UNION
                            
            SELECT p.campaign_id, NULL AS link, 
             SUM(
                CASE
                  WHEN report_download_email IS NOT NULL
                    THEN (
                    SELECT SUM(CAST(JSON_UNQUOTE(JSON_EXTRACT(json_obj, '$.parts')) AS UNSIGNED))
                      FROM (
                      SELECT TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(CONCAT(report_download_email, '-'), '-', numbers.n), '-', -1)) AS json_obj
                        FROM (
                        SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
                        ) AS numbers
                          WHERE TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(CONCAT(report_download_email, '-'), '-', numbers.n), '-', -1)) != ''
                          AND JSON_VALID(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(CONCAT(report_download_email, '-'), '-', numbers.n), '-', -1)))
                        ) AS json_objects
                      )
                      ELSE 0
                  END
              ) AS sms_parts
            FROM ${db}.participant p
            JOIN ${db}.view v ON p.view_key = v.view_key
            WHERE p.created BETWEEN ? AND ?
            GROUP BY p.campaign_id
            `,
          [startDate, endDate, startDate, endDate]
        );

        //Giftcards_sent fetch data from the participant and check if the view_key matches the one in the view table.
        const [giftcardsSendResult] = await poolConnection.query(
          `
        SELECT 
        p.campaign_id,
        CASE
        WHEN v.view_r = 'null' AND v.banner_id IS NOT NULL THEN b.title  -- Fall 1
        ELSE v.view_r  -- Fall 2 & 4
        END AS link,
        COUNT(*) AS giftcards_sent
        FROM ${db}.participant p
        JOIN ${db}.view v ON p.view_key = v.view_key
        LEFT JOIN ${db}.banner b ON v.banner_id = b.id
        WHERE 
        p.coupon_send = 1
        AND DATE(p.created) BETWEEN ? AND ?
        AND NOT (
        (v.view_r IS NULL OR TRIM(v.view_r) = '')
        AND v.banner_id IS NULL
        )
        GROUP BY p.campaign_id, link

  
        UNION
                
        SELECT p.campaign_id, NULL AS link, 
  
        COUNT(*) AS giftcards_sent
  
        FROM ${db}.participant p
        WHERE p.coupon_send = 1
        AND DATE(p.created) BETWEEN ? AND ?
        GROUP BY p.campaign_id
        `,
        [startDate, endDate, startDate, endDate]
        );

        //money_received fetch data from the participant and check if the view_key matches the one in the view table.
        const [moneyReceivedResult] = await poolConnection.query(
          `
        SELECT 
        p.campaign_id,
        CASE
        WHEN v.view_r = 'null' AND v.banner_id IS NOT NULL THEN b.title
        ELSE v.view_r
        END AS link,
        SUM(amount) AS money_received
        FROM ${db}.participant p
        JOIN ${db}.view v ON p.view_key = v.view_key
        LEFT JOIN ${db}.banner b ON v.banner_id = b.id
        WHERE p.status = 'PAID'
        AND DATE(p.created) BETWEEN ? AND ?
        AND NOT (
        (v.view_r IS NULL OR TRIM(v.view_r) = '') 
        AND v.banner_id IS NULL
        )
        GROUP BY p.campaign_id, link

                
        UNION 
                
        SELECT p.campaign_id, NULL AS link, 
  
        SUM(amount) AS money_received
  
        FROM ${db}.participant p
        WHERE p.status = 'PAID'
        AND DATE(p.created) BETWEEN ? AND ?
        GROUP BY p.campaign_id
        `,
        [startDate, endDate, startDate, endDate]
        );

        //avarage_payment fetch data from the participant and check if the view_key matches the one in the view table.
        // const [avaragePaymentResult] = await poolConnection.query(   //   `
        // SELECT p.campaign_id, v.view_r AS link,

        // ROUND(SUM(CASE WHEN status = 'PAID' THEN amount ELSE 0 END), 2) AS money_received,
        // COUNT(CASE WHEN status = 'PAID' THEN 1 END) AS paid_leads,
        // ROUND(COALESCE(SUM(CASE WHEN status = 'PAID' THEN amount ELSE 0 END) / NULLIF(COUNT(CASE WHEN status = 'PAID' THEN 1 END), 0), 0), 2) AS avarage_payment

        // FROM ${db}.participant p
        // JOIN ${db}.view v ON p.view_key = v.view_key
        // WHERE p.created BETWEEN ? AND ?
        // GROUP BY p.campaign_id, link

        // UNION
        // SELECT p.campaign_id, NULL AS link,

        // ROUND(SUM(CASE WHEN status = 'PAID' THEN amount ELSE 0 END), 2) AS money_received,
        // COUNT(CASE WHEN status = 'PAID' THEN 1 END) AS paid_leads,
        // ROUND(COALESCE(SUM(CASE WHEN status = 'PAID' THEN amount ELSE 0 END) / NULLIF(COUNT(CASE WHEN status = 'PAID' THEN 1 END), 0), 0), 2) AS avarage_payment

        // FROM ${db}.participant p
        // WHERE p.created BETWEEN ? AND ?
        // GROUP BY p.campaign_id
        // `,
        // [startDate, endDate, startDate, endDate]);

        //engagement_time fetch data from the participant and check if the view_key matches the one in the view table.
        const [engagementTimeResult] = await poolConnection.query(
          `
        SELECT 
        p.campaign_id,
        CASE
        WHEN v.view_r = 'null' AND v.banner_id IS NOT NULL THEN b.title
        ELSE v.view_r
        END AS link,
        SUM(time_spent) AS engagement_time
        FROM ${db}.participant p
        JOIN ${db}.view v ON p.view_key = v.view_key
        LEFT JOIN ${db}.banner b ON v.banner_id = b.id
        WHERE DATE(p.created) BETWEEN ? AND ?
        AND NOT (
        (v.view_r IS NULL OR TRIM(v.view_r) = '') 
        AND v.banner_id IS NULL
        )
        GROUP BY p.campaign_id, link

  
        UNION 
                
        SELECT p.campaign_id, NULL AS link, 
          
        SUM(time_spent) AS engagement_time 
        FROM ${db}.participant p
        WHERE DATE(p.created) BETWEEN ? AND ?
        GROUP BY p.campaign_id
        `,
        [startDate, endDate, startDate, endDate]
        );

        //answers_percentage fetch data from the participant and check if the view_key matches the one in the view table.
        const [answersPercentageResult] = await poolConnection.query(
          `
        SELECT 
        p.campaign_id,
        CASE
        WHEN v.view_r = 'null' AND v.banner_id IS NOT NULL THEN b.title
        ELSE v.view_r
        END AS link,
        COUNT(*) AS answers_percentage
        FROM ${db}.participant p
        JOIN ${db}.view v ON p.view_key = v.view_key
        LEFT JOIN ${db}.banner b ON v.banner_id = b.id
        WHERE DATE(p.created) BETWEEN ? AND ?
        AND p.view_key IS NOT NULL AND p.view_key <> ''
        AND (p.status = 'ERROR' OR p.status = 'DECLINED')
        AND NOT (
        (v.view_r IS NULL OR TRIM(v.view_r) = '')
        AND v.banner_id IS NULL
        )
        GROUP BY p.campaign_id, link

  
        UNION 
  
        SELECT p.campaign_id, NULL AS link, 
        COUNT(*) AS answers_percentage
  
        FROM ${db}.participant p
        WHERE DATE(p.created) BETWEEN ? AND ?
  
        AND (p.status = 'ERROR' OR p.status = 'DECLINED')
        GROUP BY p.campaign_id, link
        `,
        [startDate, endDate, startDate, endDate]
        );


        // //flow_mode_id fetch data from the view and check if the campaign_id from view matches the id in the campaign table.
        const [flowModeIdResult] = await poolConnection.query(  `
        SELECT v.campaign_id, v.view_r AS link,
        c.flow_mode_id AS flow_mode_id
        
        FROM ${db}.view v
        JOIN ${db}.campaign c ON v.campaign_id = c.id
        WHERE DATE(v.created) BETWEEN ? AND ?
        
        GROUP BY v.campaign_id, link
        `,
        [startDate, endDate]);

        // //campaign_name fetch data from the view and check if the campaign_id from view matches the id in the campaign table.
        const [campaignNameResult] = await poolConnection.query(  `
        SELECT 
          v.campaign_id, 
          v.view_r AS link,
          LEFT(c.name, 100) AS campaign_name
        FROM ${db}.view v
        JOIN ${db}.campaign c ON v.campaign_id = c.id
        WHERE DATE(v.created) BETWEEN ? AND ?
        GROUP BY v.campaign_id, link
          `,
        [startDate, endDate]);

        if (
          !viewLinkResult?.length &&
          !viewCampaignResult?.length &&
          !leadsResult?.length &&
          !paidleadsResult?.length &&
          !uniqueLeadsResult?.length &&
          !recuringLeadsResult?.length &&
          !smsPartsResult?.length &&
          !giftcardsSendResult?.length &&
          !moneyReceivedResult?.length &&
          !engagementTimeResult?.length &&
          !answersPercentageResult?.length
          (!campaignNameResult?.length) &&
          (!flowModeIdResult?.length)
        ) {
          console.log(`Ingen relevant data hittades för ${db}, hoppar över.`);
          continue;
        }
      
        results.push({
          db,
          success: true,
          startDate,
          endDate,
          viewLinkResult,
          viewCampaignResult,
          leadsResult,
          paidleadsResult,
          uniqueLeadsResult,
          recuringLeadsResult,
          smsPartsResult,
          giftcardsSendResult,
          moneyReceivedResult,
          // avaragePaymentResult,
          engagementTimeResult,
          answersPercentageResult,
          flowModeIdResult,
          campaignNameResult,
        });

        console.log(`Process klar för ${db}`);
      } catch (err) {
        console.error(`Fel vid körning av ${db}:`, err);
        results.push({ db, success: false, error: err.message });
      }
    }

    return results;
  } catch (error) {
    console.error("Allvarligt fel i reportService:", error);
    throw error;
  }
};
