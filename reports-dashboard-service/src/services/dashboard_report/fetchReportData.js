    import { reportService } from "./reportService.js";
    import { dbConnection } from "../../config/db.js";
    import { createDataLine } from "./updateMergeData.js";
    import { updateMergeData } from "./updateMergeData.js";

    export const fetchReportData = async () => {
      let batchSize = 100;

      try {
        //Fetches the database connection and all databases, including those that are filtered out.
        const { poolConnection, filteredDatabases } = await dbConnection();

        // Fetches all data from reportService from all SQL queries, e.g. leads, paid_leads, unique_leads, etc.
        const allResults = await reportService();

        //This is the date to display the date of the fetched data in the date column.
        const { startDate, endDate} = allResults[0];

        //An array to collect all data for all links with the same campaign_id.
        const mergeDataByLink = [];

        //An array to collect all data for same campaign_id.
        const mergeDataPerCampaign = [];

        // Here I loop through all the data from my queries in reportService.
        allResults.forEach((results) => {
          const dbdb = results.db;

          //Here, I loop through the data that shows from the view table displaying all the links and views with the same campaign_id.
          results.viewLinkResult.forEach((row) => {
            const link = row.link || null;
            const key = `${row.campaign_id}-${link}`;

            mergeDataByLink.push({
              db: dbdb,
              key: key,
              campaign_id: row.campaign_id,
              link: link,
              views: row.views,
            });
          });

          //Here, I loop through the view table that shows the number of links and views for an entire campaign with the same campaign_id.
          results.viewCampaignResult.forEach((row) => {
            const key = row.campaign_id;
            mergeDataPerCampaign.push({
              db: dbdb,
              key: key,
              campaign_id: row.campaign_id,
              link: "Total links: " + row.link,
              views: row.views,
            });
          });

          // Here I put all the data from leads, paid_leads, unique_leads, recurring_leads, giftcards_sent, money_received, sms_parts, average_payment, engagement_time, answers_percentage into an array.
          const combinedData = [
            ...results.leadsResult,
            ...results.paidleadsResult,
            ...results.uniqueLeadsResult,
            ...results.recuringLeadsResult,
            ...results.smsPartsResult,
            ...results.giftcardsSendResult,
            ...results.moneyReceivedResult,
            // ...results.avaragePaymentResult,
            ...results.engagementTimeResult,
            ...results.answersPercentageResult,
            ...results.flowModeIdResult,
            ...results.campaignNameResult,
          ];

          //Here I loop through the combined data array,
          combinedData.forEach((row) => {
            const campaign_id = row.campaign_id;
            const link = row.link || null;

            //If I check if location matches view_r, if it does, we add leads, paid_leads, etc....
            const existingEntry = mergeDataByLink.find(
              (entry) =>
                entry.db === dbdb &&
                entry.campaign_id === campaign_id &&
                entry.link === link
            );

            //If I check if campaign_id matches campaign_id in both participant and view, if it does, we add leads, paid_leads, etc....
            const existingEntryCampaign = mergeDataPerCampaign.find(
              (entry) => entry.db === dbdb && entry.campaign_id === campaign_id
            );

            if (existingEntry) {
              updateMergeData(existingEntry, row);
            }

            if (existingEntryCampaign) {
              updateMergeData(existingEntryCampaign, row);
            }
          });
        });

        //Här loppar jag igenom databaserna gör att lägga till rätt data i rätt databas
        for (const db of filteredDatabases) {
          const dataPerLinkAndPerCampaign = [];

          //Here, I am checking that the data from mergeDataByLink and mergeDataPerCampaign matches the correct database to avoid incorrect data in the wrong database.
          const linkDataDb = mergeDataByLink.filter((entry) => entry.db === db);
          const campaignDataDb = mergeDataPerCampaign.filter(
            (entry) => entry.db === db
          );

          /* Here, I loop through linkDataDb and campaignDataDb to transform the data  
          using createDataLine and store it in dataPerLinkAndPerCampaign,  
          preparing it for batch insertion. */
          linkDataDb.forEach((data) => {
            dataPerLinkAndPerCampaign.push(createDataLine(data, startDate, endDate));
          });
          campaignDataDb.forEach((data) => {
            dataPerLinkAndPerCampaign.push(createDataLine(data, startDate, endDate));
          });

          // If dataPerLinkAndPerCampaign is greater than 0, we add the data
          if (dataPerLinkAndPerCampaign.length > 0) {
            // Here we sort so that the total for the entire campaign ends up just below the last link.
            dataPerLinkAndPerCampaign.sort((a, b) => {
              if (a[1] !== b[1]) {
                return a[1] - b[1];
              }
              return a[2] === null ? -1 : b[2] === null ? 1 : 0;
            });

            let batch = [];

            let currentIndex = 0;
            const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

            const processBatch = async () => {
              batch = [];
              for (
                let i = 0;
                i < batchSize && currentIndex < dataPerLinkAndPerCampaign.length;
                i++
              ) {
                batch.push(dataPerLinkAndPerCampaign[currentIndex]);
                currentIndex++;
              }

              if (batch.length > 0) {
                console.log("Batch", batch.length, " and index", currentIndex);

                try {
                  await poolConnection.query(
                    `
                    INSERT INTO ${db}.dashboard_report
                    (date, campaign_id, link, views, leads, paid_leads, unique_leads, recuring_leads, sms_parts, giftcards_sent, money_received, engagement_time, answers_percentage, created, emails_delivered, quiz_finished)
                    VALUES ?
                  `,
                    [batch]
                  );

                  console.log(`Data inserted into ${db}`);
                } catch (insertError) {
                  console.error(`Error during INSERT in ${db}:`, insertError);
                }

                batch = [];
              }
              if (currentIndex < dataPerLinkAndPerCampaign.length) {
                console.log("Väntar på nästa batch");

                await delay(10000);
                await processBatch();
              } else {
                console.log("All data har bearbetats");
              }
            };
            await processBatch();
          } else {
            console.log(`No data to insert into ${db}`);
          }
        }
      } catch (error) {
        console.error("Error vid hämtning av rapportdata:", error);
      }
    };
