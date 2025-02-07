import { dbConnection } from "../config/db.js";

export const reportService = async () => {
  try {
    const pool = await dbConnection();

    const [viewRows] = await pool.query(`
            SELECT campaign_id, view_r AS link, COUNT(view_r) AS views
            FROM view
            WHERE created >= DATE_SUB(NOW(), INTERVAL 1 DAY)
            GROUP BY campaign_id, view_r
        `);

    const [viewRows2] = await pool.query(`
            SELECT campaign_id, COUNT(view_r) AS link,
            COUNT(view_r) AS views
            FROM view 
            WHERE created >= DATE_SUB(NOW(), INTERVAL 1 DAY)
            GROUP BY campaign_id
        `);

    if (viewRows.length === 0 || viewRows2.length === 0) {
      return;
    }

    const [LeadsAndPaidLeads] = await pool.query(`
            SELECT p.campaign_id, p.location,
            COUNT(CASE WHEN p.telephone IS NOT NULL OR p.name IS NOT NULL OR p.email IS NOT NULL THEN 1 END) AS leads,
            COUNT(CASE WHEN p.status = 'PAID' THEN 1 END) AS paid_leads
            FROM participant p
            GROUP BY p.campaign_id, p.location

            UNION 

            SELECT p.campaign_id, NULL AS location,
            COUNT(CASE WHEN p.telephone IS NOT NULL OR p.name IS NOT NULL OR p.email IS NOT NULL THEN 1 END) AS leads,
            COUNT(CASE WHEN p.status = 'PAID' THEN 1 END) AS paid_leads
            FROM participant p
            GROUP BY p.campaign_id

        `);

    const [countUniqueLeads] = await pool.query(`
            SELECT campaign_id, location,
            COUNT(DISTINCT CASE WHEN status = 'PAID' THEN telephone END) AS unique_leads, 
            COUNT(DISTINCT CASE WHEN custom_text4 = 'ACTIVE' THEN telephone END) AS recuring_leads
            FROM participant
            WHERE telephone IS NOT NULL
            GROUP BY campaign_id, location

            UNION

            SELECT campaign_id, NULL AS location,
            COUNT(DISTINCT CASE WHEN status = 'PAID' THEN telephone END) AS unique_leads, 
            COUNT(DISTINCT CASE WHEN custom_text4 = 'ACTIVE' THEN telephone END) AS recuring_leads
            FROM participant
            WHERE telephone IS NOT NULL
            GROUP BY campaign_id
        `);

    const [summUpMoneyReceived] = await pool.query(`
            SELECT campaign_id, location, 
            COUNT(*) AS giftcards_sent
            FROM participant
            WHERE coupon_sent = 1
            GROUP BY campaign_id, location

            UNION

            SELECT campaign_id, NULL AS location, 
            COUNT(*) AS giftcards_sent
            FROM participant
            WHERE coupon_sent = 1
            GROUP BY campaign_id

        `);

    const [moneyMeceivedResult] = await pool.query(`
            SELECT campaign_id, location,
            SUM(amount) AS money_received
            FROM participant
            GROUP BY campaign_id, location

            UNION

            SELECT campaign_id, NULL AS location,
            SUM(amount) AS money_received
            FROM participant
            GROUP BY campaign_id
        `);

    const [avaragePaymentResult] = await pool.query(`
            SELECT 
            campaign_id, location,
            SUM(amount) AS money_received,
            COUNT(CASE WHEN status = 'PAID' THEN 1 END) AS paid_leads,
            CASE 
                WHEN COUNT(CASE WHEN status = 'PAID' THEN 1 END) > 0 THEN SUM(amount) / COUNT(CASE WHEN status = 'PAID' THEN 1 END)
                ELSE 0
            END AS avarage_payment
            FROM participant
            GROUP BY campaign_id, location

            UNION 

            SELECT campaign_id, NULL AS location,
            SUM(amount) AS money_received,
            COUNT(CASE WHEN status = 'PAID' THEN 1 END) AS paid_leads,
            CASE 
                WHEN COUNT(CASE WHEN status = 'PAID' THEN 1 END) > 0 THEN SUM(amount) / COUNT(CASE WHEN status = 'PAID' THEN 1 END)
                ELSE 0
            END AS avarage_payment
            FROM participant
            GROUP BY campaign_id
            `);

    return {
      viewRows,
      viewRows2,
      LeadsAndPaidLeads,
      countUniqueLeads,
      summUpMoneyReceived,
      moneyMeceivedResult,
      avaragePaymentResult,
    };
  } catch (error) {
    console.error("Error under databasoperation:", error);
  }
};
