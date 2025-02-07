import { reportService } from "./reportService.js";
import { dbConnection } from "../config/db.js";

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

    combinedData.push(...LeadsAndPaidLeads);
    combinedData.push(...countUniqueLeads);
    combinedData.push(...summUpMoneyReceived);
    combinedData.push(...moneyMeceivedResult);
    combinedData.push(...avaragePaymentResult);

    const mergeData = {};
    const mergeDataPerCampaign = {};

    viewResult.forEach((row) => {
      const key = `${row.campaign_id}-${row.link}`;

      // console.log(`View key: ${key}`);
      // console.log(`View row: ${JSON.stringify(row)}`);

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

      //   console.log("Rows", row);

      // Uppdatera mergeData för den aktuella nyckeln
      if (mergeData[key]) {
        // Här summeras inte längre leads, utan vi uppdaterar fälten direkt från SQL
        mergeData[key].leads =
          row.leads !== undefined ? row.leads : mergeData[key].leads;
        mergeData[key].paid_leads =
          row.paid_leads !== undefined
            ? row.paid_leads
            : mergeData[key].paid_leads;
        mergeData[key].unique_leads =
          row.unique_leads !== undefined
            ? row.unique_leads
            : mergeData[key].unique_leads;
        mergeData[key].recuring_leads =
          row.recuring_leads !== undefined
            ? row.recuring_leads
            : mergeData[key].recuring_leads;
        mergeData[key].giftcards_sent =
          row.giftcards_sent !== undefined
            ? row.giftcards_sent
            : mergeData[key].giftcards_sent;
        mergeData[key].money_received =
          row.money_received !== undefined
            ? row.money_received
            : mergeData[key].money_received;
        mergeData[key].avarage_payment =
          row.avarage_payment !== undefined
            ? row.avarage_payment
            : mergeData[key].avarage_payment;

        // Beräkna conversion rate
        mergeData[key].conversion_rate =
          mergeData[key].views > 0
            ? (mergeData[key].leads / mergeData[key].views) * 100
            : 0;
      }

      if (mergeDataPerCampaign[campaign_key]) {
        if (row.location === null) {
          mergeDataPerCampaign[campaign_key].leads =
            row.leads !== undefined
              ? row.leads
              : mergeDataPerCampaign[campaign_key].leads;
          mergeDataPerCampaign[campaign_key].paid_leads =
            row.paid_leads !== undefined
              ? row.paid_leads
              : mergeDataPerCampaign[campaign_key].paid_leads;
          mergeDataPerCampaign[campaign_key].unique_leads =
            row.unique_leads !== undefined
              ? row.unique_leads
              : mergeDataPerCampaign[campaign_key].unique_leads;
          mergeDataPerCampaign[campaign_key].recuring_leads =
            row.recuring_leads !== undefined
              ? row.recuring_leads
              : mergeDataPerCampaign[campaign_key].recuring_leads;
          mergeDataPerCampaign[campaign_key].giftcards_sent =
            row.giftcards_sent !== undefined
              ? row.giftcards_sent
              : mergeDataPerCampaign[campaign_key].giftcards_sent;
          mergeDataPerCampaign[campaign_key].money_received =
            row.money_received !== undefined
              ? row.money_received
              : mergeDataPerCampaign[campaign_key].money_received;
          mergeDataPerCampaign[campaign_key].avarage_payment =
            row.avarage_payment !== undefined
              ? row.avarage_payment
              : mergeDataPerCampaign[campaign_key].avarage_payment;

          mergeDataPerCampaign[campaign_key].conversion_rate =
            mergeDataPerCampaign[campaign_key].views > 0
              ? (mergeDataPerCampaign[campaign_key].leads /
                  mergeDataPerCampaign[campaign_key].views) *
                100
              : 0;
        }
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
