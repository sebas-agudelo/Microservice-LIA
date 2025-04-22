export const updateMergeData = (target, row) => {
  target.leads = (row.leads !== undefined && row.leads !== null && !isNaN(row.leads)) ? row.leads : target.leads;
  target.paid_leads = (row.paid_leads !== undefined && row.paid_leads !== null && !isNaN(row.paid_leads)) ? row.paid_leads : target.paid_leads;
  target.unique_leads = (row.unique_leads !== undefined && row.unique_leads !== null && !isNaN(row.unique_leads)) ? row.unique_leads : target.unique_leads;
  target.recuring_leads = (row.recuring_leads !== undefined && row.recuring_leads !== null && !isNaN(row.recuring_leads)) ? row.recuring_leads : target.recuring_leads;
  target.sms_parts = (row.sms_parts !== undefined && row.sms_parts !== null && !isNaN(row.sms_parts)) ? row.sms_parts : target.sms_parts;
  target.giftcards_sent = (row.giftcards_sent !== undefined && row.giftcards_sent !== null && !isNaN(row.giftcards_sent)) ? row.giftcards_sent : target.giftcards_sent;
  target.money_received = (row.money_received !== undefined && row.money_received !== null && !isNaN(row.money_received)) ? row.money_received : target.money_received;
  // target.avarage_payment = (row.avarage_payment !== undefined && row.avarage_payment !== null && !isNaN(row.avarage_payment)) ? row.avarage_payment : target.avarage_payment;
  target.engagement_time = (row.engagement_time !== undefined && row.engagement_time !== null && !isNaN(row.engagement_time)) ? row.engagement_time : target.engagement_time;
  target.answers_percentage = (row.answers_percentage !== undefined && row.answers_percentage !== null && !isNaN(row.answers_percentage)) ? row.answers_percentage : target.answers_percentage;
  target.campaign_name = (row.campaign_name !== undefined && row.campaign_name !== null) ? row.campaign_name : target.campaign_name;
  target.flow_mode_id = (row.flow_mode_id !== undefined && row.flow_mode_id !== null && !isNaN(row.flow_mode_id)) ? row.flow_mode_id : target.flow_mode_id;
  // target.conversion_rate = target.views > 0 ? (target.leads / target.views) * 100 : 0;
};

export const createDataLine = (data, startDate, endDate) => {
  const filtredDat =` ${startDate} - ${endDate}`;

  const SwedishTimeZone = new Date().toLocaleString("sv-SE", { timeZone: "Europe/Stockholm" });

  return [
      filtredDat,
      data.campaign_id || 0,
      data.link || 0,
      data.views || 0, 
      data.leads || 0, 
      data.paid_leads || 0, 
      data.unique_leads || 0, 
      data.recuring_leads || 0, 
      data.sms_parts || 0,
      // data.conversion_rate ? data.conversion_rate + "%" : 0 + "%", 
      data.giftcards_sent || 0, 
      data.money_received || 0, 
      // data.avarage_payment ? data.avarage_payment + "%" : 0 + "%", 
      data.engagement_time || 0, 
      data.answers_percentage || 0, 
      SwedishTimeZone,
      data.campaign_name || null, 
      data.flow_mode_id || null, 
    ];
}