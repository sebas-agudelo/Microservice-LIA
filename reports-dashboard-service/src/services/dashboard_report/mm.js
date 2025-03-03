  //       else if (existingEntry2) {

  //         if (leads !== 0 || paid_leads !== 0 || unique_leads !== 0 || giftcards_sent !== 0 || money_received !== 0 || avarage_payment !== 0 || engagement_time !== 0 || answers_percentage !== 0) {
  //           if (leads !== 0) {
  //               existingEntry2.leads = leads;
  //               console.log('Updated leads campaign:', existingEntry2.leads);
  //           }
  //           if (paid_leads !== 0) {
  //             existingEntry2.paid_leads = paid_leads;
  //               console.log('Updated paid_leads campaign:', existingEntry2.paid_leads);
  //           }
  //           if (unique_leads !== 0) {
  //             existingEntry2.unique_leads = unique_leads;
  //               console.log('Updated unique_leads campaign:', existingEntry2.unique_leads);
  //           }
  //           if (giftcards_sent !== 0) {
  //             existingEntry2.giftcards_sent = giftcards_sent;
  //             console.log('Updated giftcards_sent campaign:', existingEntry2.giftcards_sent);
  //         }
  //         if (money_received !== 0) {
  //           existingEntry2.money_received = money_received;
  //           console.log('Updated money_received campaign:', existingEntry2.money_received);
  //       }
  //       if (avarage_payment !== 0) {
  //         existingEntry2.avarage_payment = avarage_payment;
  //         console.log('Updated avarage_payment campaign:', existingEntry2.avarage_payment);
  //     }
  //     if (engagement_time !== 0) {
  //       existingEntry2.engagement_time = engagement_time;
  //       console.log('Updated engagement_time:', existingEntry2.engagement_time);
  //   }
  //   if (answers_percentage !== 0) {
  //     existingEntry2.answers_percentage = answers_percentage;
  //     console.log('Updated answers_percentage:', existingEntry2.answers_percentage);
  // }
  //       }
          
  //       }

//   combinedData.forEach((row) => {
//     const locationKey = row.location ?? null;

//     const leads = (row.leads !== undefined && row.leads !== null && !isNaN(row.leads)) ? row.leads : 0;
//     const paid_leads = (row.paid_leads !== undefined && row.paid_leads !== null && !isNaN(row.paid_leads)) ? row.paid_leads : 0;
//     const unique_leads = (row.unique_leads !== undefined && row.unique_leads !== null && !isNaN(row.unique_leads)) ? row.unique_leads : 0;
//     const giftcards_sent = (row.giftcards_sent !== undefined && row.giftcards_sent !== null && !isNaN(row.giftcards_sent)) ? row.giftcards_sent : 0;
//     const money_received = (row.money_received !== undefined && row.money_received !== null && !isNaN(row.money_received)) ? row.money_received : 0;
//     const avarage_payment = (row.avarage_payment !== undefined && row.avarage_payment !== null && !isNaN(row.avarage_payment)) ? row.avarage_payment : 0;
//     const engagement_time = (row.engagement_time !== undefined && row.engagement_time !== null && !isNaN(row.engagement_time)) ? row.engagement_time : 0;
//     const answers_percentage  = (row.answers_percentage  !== undefined && row.answers_percentage  !== null && !isNaN(row.answers_percentage )) ? row.answers_percentage  : 0;
//     // const unique_leads = (row.unique_leads !== undefined && row.unique_leads !== null && !isNaN(row.unique_leads)) ? row.unique_leads : 0;

//     const existingEntry = mergeDataPerLink.find(
//         (entry) => entry.db === dbdb &&
//                    entry.campaign_id === row.campaign_id &&
//                    entry.link === locationKey
//     );

//     const existingEntry2 = mergeDataPerCampaign.find(
//       (entry) => entry.db === dbdb && entry.campaign_id === row.campaign_id
//     );

//     const existingEntry3 = mergeDataPerLink.find(
//       (entry) => entry.db === dbdb && entry.campaign_id === row.campaign_id && entry.link !== locationKey
//   );

//     if (existingEntry) {
        
//       if (leads !== 0 || paid_leads !== 0 || unique_leads !== 0 || giftcards_sent !== 0 || money_received !== 0 || avarage_payment !== 0 || engagement_time !== 0 || answers_percentage !== 0) {
//           if (leads !== 0) {
//             existingEntry.leads = leads;
//             // console.log('Updated leads:', existingEntry.leads);
//         }
//           if (paid_leads !== 0) {
//             existingEntry.paid_leads = paid_leads;
//             // console.log('Updated paid_leads:', existingEntry.paid_leads);
//         }
//           if (unique_leads !== 0) {
//             existingEntry.unique_leads = unique_leads;
//             // console.log('Updated unique_leads:', existingEntry.unique_leads);
//         }
//           if (giftcards_sent !== 0) {
//           existingEntry.giftcards_sent = giftcards_sent;
//           // console.log('Updated giftcards_sent:', existingEntry.giftcards_sent);
//         }
//           if (money_received !== 0) {
//         existingEntry.money_received = money_received;
//         // console.log('Updated money_received:', existingEntry.money_received);
//         }
//           if (avarage_payment !== 0) {
//       existingEntry.avarage_payment = avarage_payment;
//       // console.log('Updated avarage_payment:', existingEntry.avarage_payment);
//   }
//   if (engagement_time !== 0) {
//     existingEntry.engagement_time = engagement_time;
//     // console.log('Updated engagement_time:', existingEntry.engagement_time);
// }
// if (answers_percentage !== 0) {
//   existingEntry.answers_percentage = answers_percentage;
//   console.log('Updated answers_percentage existingEntry:', existingEntry.answers_percentage);
// }
//     }
//     }

//     else if(!existingEntry && existingEntry3){
      
//       if (answers_percentage === 0) {
//         existingEntry3.answers_percentage = answers_percentage;
//         console.log('Updated answers_percentage existingEntry3:', existingEntry3.answers_percentage);
//     }
//     }

// //       else if (existingEntry2) {

// //         if (leads !== 0 || paid_leads !== 0 || unique_leads !== 0 || giftcards_sent !== 0 || money_received !== 0 || avarage_payment !== 0 || engagement_time !== 0 || answers_percentage !== 0) {
// //           if (leads !== 0) {
// //               existingEntry2.leads = leads;
// //               console.log('Updated leads campaign:', existingEntry2.leads);
// //           }
// //           if (paid_leads !== 0) {
// //             existingEntry2.paid_leads = paid_leads;
// //               console.log('Updated paid_leads campaign:', existingEntry2.paid_leads);
// //           }
// //           if (unique_leads !== 0) {
// //             existingEntry2.unique_leads = unique_leads;
// //               console.log('Updated unique_leads campaign:', existingEntry2.unique_leads);
// //           }
// //           if (giftcards_sent !== 0) {
// //             existingEntry2.giftcards_sent = giftcards_sent;
// //             console.log('Updated giftcards_sent campaign:', existingEntry2.giftcards_sent);
// //         }
// //         if (money_received !== 0) {
// //           existingEntry2.money_received = money_received;
// //           console.log('Updated money_received campaign:', existingEntry2.money_received);
// //       }
// //       if (avarage_payment !== 0) {
// //         existingEntry2.avarage_payment = avarage_payment;
// //         console.log('Updated avarage_payment campaign:', existingEntry2.avarage_payment);
// //     }
// //     if (engagement_time !== 0) {
// //       existingEntry2.engagement_time = engagement_time;
// //       console.log('Updated engagement_time:', existingEntry2.engagement_time);
// //   }
// //   if (answers_percentage !== 0) {
// //     existingEntry2.answers_percentage = answers_percentage;
// //     console.log('Updated answers_percentage:', existingEntry2.answers_percentage);
// // }
// //       }
      
// //       }

   
// });