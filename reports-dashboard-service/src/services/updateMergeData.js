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


