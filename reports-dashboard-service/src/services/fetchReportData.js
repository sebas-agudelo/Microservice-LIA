import { reportService } from "./reportService.js";
import { dbConnection } from "../config/db.js";
import { updateMergeData } from "./updateMergeData.js";

export const fetchReportData = async () => {
  try {
    const pool = await dbConnection();
    const results = await reportService();

    if (!results) {
      console.log("reportService returnerade inget resultat");
      return;
    }

    const viewResult = results.viewRows;
    const viewResult2 = results.viewRows2;
    const LeadsAndPaidLeads = results.LeadsAndPaidLeads;
    const countUniqueLeads = results.countUniqueLeads;
    const summUpMoneyReceived = results.summUpMoneyReceived;
    const moneyMeceivedResult = results.moneyMeceivedResult;
    const avaragePaymentResult = results.avaragePaymentResult;

    const combinedData = [];

    combinedData.push(
        ...LeadsAndPaidLeads, 
        ...countUniqueLeads, 
        ...summUpMoneyReceived, 
        ...moneyMeceivedResult, 
        ...avaragePaymentResult
    );

    const mergeData = {};
    const mergeDataPerCampaign = {};

    viewResult.forEach((row) => {
      const key = `${row.campaign_id}-${row.link}`;

      if (!mergeData[key]) {
        mergeData[key] = {
          campaign_id: row.campaign_id,
          link: row.link,
          views: row.views,
          leads: 0,
          paid_leads: 0,
          unique_leads: 0,
          recuring_leads: 0,
          giftcards_sent: 0,
          money_received: 0,
          average_payment: 0,
          conversion_rate: 0,
        };
      }
    });

    viewResult2.forEach((row) => {
      const campaign_key = `${row.campaign_id}`;

      if (!mergeDataPerCampaign[campaign_key]) {
        mergeDataPerCampaign[campaign_key] = {
          campaign_id: row.campaign_id,
          link: row.link,
          views: row.views,
          leads: 0,
          paid_leads: 0,
          unique_leads: 0,
          recuring_leads: 0,
          giftcards_sent: 0,
          money_received: 0,
          average_payment: 0,
          conversion_rate: 0,
        };
      }
    });

    combinedData.forEach((row) => {
      const key = `${row.campaign_id}-${row.location}`;
      const campaign_key = row.campaign_id;

      if (mergeData[key]) {
        updateMergeData(mergeData[key], row);
      }

      if (mergeDataPerCampaign[campaign_key] && row.location === null) {
        updateMergeData(mergeDataPerCampaign[campaign_key], row);
      }
    });

    const dataPerLinkAndPerCampaign = [];

    Object.values(mergeData).map((data) => {
      dataPerLinkAndPerCampaign.push([
        new Date(),
        data.campaign_id,
        data.link,
        data.views,
        data.leads,
        data.paid_leads,
        data.unique_leads,
        data.recuring_leads,
        data.conversion_rate,
        data.giftcards_sent,
        data.money_received,
        data.avarage_payment,
      ]);
    });

    Object.values(mergeDataPerCampaign).map((data) => {
      dataPerLinkAndPerCampaign.push([
        new Date(),
        data.campaign_id,
        data.link,
        data.views,
        data.leads,
        data.paid_leads,
        data.unique_leads,
        data.recuring_leads,
        data.conversion_rate,
        data.giftcards_sent,
        data.money_received,
        data.avarage_payment,
      ]);
    });

    dataPerLinkAndPerCampaign.sort((a, b) => {
      if (a[1] !== b[1]) {
        return a[1] - b[1];
      } else {
        if (a[2] === null && b[2] !== null) {
          return 1;
        } else if (a[2] !== null && b[2] === null) {
          return -1;
        } else {
          return 0;
        }
      }
    });

    try {
      await pool.query(
        `
               INSERT INTO dashboard_report (date, campaign_id, link, views, leads, paid_leads, unique_leads, recuring_leads, conversion_rate, giftcards_sent, money_received, avarage_payment)
               VALUES ?
               `,
        [dataPerLinkAndPerCampaign]
      );
    } catch (error) {
      console.error(error);
    }
  } catch (error) {
    console.error(error);
  }
};
