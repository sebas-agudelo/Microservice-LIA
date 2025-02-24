export const updateMergeData = (target, row) => {
    target.leads = row.leads ?? target.leads;
    target.paid_leads = row.paid_leads ?? target.paid_leads;
    target.unique_leads = row.unique_leads ?? target.unique_leads;
    target.recuring_leads = row.recuring_leads ?? target.recuring_leads;
    target.giftcards_sent = row.giftcards_sent ?? target.giftcards_sent;
    target.money_received = row.money_received ?? target.money_received;
    target.avarage_payment = row.avarage_payment ?? target.avarage_payment;
    target.engagement_time = row.engagement_time ?? target.engagement_time;
    target.answers_percentage = row.answers_percentage ?? target.answers_percentage;
    target.conversion_rate = target.views > 0 ? (target.leads / target.views) * 100 : 0;
};

export const createDataLine = (data) => {
    return [
        new Date(),
        data.campaign_id,
        data.link,
        data.views || 0, 
        data.leads || 0, 
        data.paid_leads || 0, 
        data.unique_leads || 0, 
        data.recuring_leads || 0, 
        data.conversion_rate ? data.conversion_rate + "%" : 0 + "%", 
        data.giftcards_sent || 0, 
        data.money_received || 0, 
        data.avarage_payment ? data.avarage_payment + "%" : 0 + "%", 
        data.engagement_time || 0, 
        data.answers_percentage  || 0
      ];
}
