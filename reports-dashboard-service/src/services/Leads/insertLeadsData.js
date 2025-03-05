import { dbConnection } from "../../config/db.js";

export default async function mergeData() {
  try {
    const { poolConnection, filteredDatabases } = await dbConnection();
    for (const db of filteredDatabases) {
      const [participants] = await poolConnection.query(
        `SELECT id, telephone, location, amount, name, email, coupon_send, address, postalcode, city, personal_number, points_scored, time_spent, sms_parts, sms_cost, agree_download_report, custom_text4, receiver_phone, recurring_history, game_type, Custom_timestamp_3, receiver_phone, created FROM ${db}.participant WHERE created >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)`
      );

      const mergedData = {};

      const normalizePhone = (phone) => {
        if (!phone) return;

        // Sverige
        if (phone.startsWith("07")) return "46" + phone.slice(1);
        if (phone.startsWith("46")) return "46" + phone.slice(2);

        // Norge
        if (phone.startsWith("4") && !phone.startsWith("47")) {
          return "474" + phone.slice(1);
        }
        if (phone.startsWith("9")) {
          return "479" + phone.slice(1);
        }
        if (phone.startsWith("47")) {
          return "47" + phone.slice(2);
        }

        // Finland
        if (
          phone.startsWith("040") ||
          phone.startsWith("041") ||
          phone.startsWith("044") ||
          phone.startsWith("045") ||
          phone.startsWith("046") ||
          phone.startsWith("050")
        ) {
          return "358" + phone.slice(1);
        }

        if (phone.startsWith("358")) {
          return "358" + phone.slice(3);
        }

        // Danmark
        if (
          phone.startsWith("30") ||
          phone.startsWith("40") ||
          (phone.startsWith("50") && !phone.startsWith("45"))
        ) {
          return "45" + phone;
        }

        if (phone.startsWith("45")) {
          return "45" + phone.slice(2);
        }

        return phone;
      };

      // Process participant data as before...
      participants.forEach((participant) => {
        let phone = normalizePhone(participant.telephone);
        if (!phone) return;

        if (!mergedData[phone]) {
          mergedData[phone] = {
            locations: new Set(),
            participants_id: new Set(),
            total_amount: 0,
            giftcards_sent: 0,
            name: participant.name,
            email: participant.email,
            address: participant.address,
            zip: participant.postcode,
            city: participant.city,
            personal_number: participant.personal_number,
            quiz_answers: participant.points_scored,
            custom_field_1: 0,
            custom_field_2: 0,
            custom_field_3: 0,
            custom_field_4: 0,
            custom_field_5: 0,
            affiliated_views_generated: 0,
            affiliated_leads_generated: null,
            affiliated_money_generated: 0,
            tags: 0,
            all_dates: 0,
            gameTypes: new Set(),
            petition: null,
            newsletter: null,
            phone,
          };
        }

        const data = mergedData[phone];
        data.locations.add(participant.location);
        data.participants_id.add(participant.id);
        data.total_amount += +participant.amount;
        data.giftcards_sent += +participant.coupon_send;
        data.name = participant.name;
        data.email = participant.email;
        data.zip = participant.postalcode;
        data.city = participant.city;
        data.personal_number = participant.personal_number;
        data.quiz_answers += +participant.points_scored;
        data.custom_field_1 += +participant.time_spent;
        data.custom_field_2 += +participant.sms_parts;
        data.custom_field_3 += +participant.sms_cost;
        data.custom_field_4 += +participant.agree_download_report;
        data.custom_field_5 += 1;

        const activeCount = participants.filter(
          (p) =>
            p.custom_text4 === "ACTIVE" &&
            p.recurring_history === "15" &&
            normalizePhone(p.telephone) === phone
        ).length;

        const deleteCount = participants.filter(
          (p) =>
            p.custom_text4 === "DELETED" &&
            p.recurring_history === "15" &&
            normalizePhone(p.telephone) === phone
        ).length;

        data.affiliated_views_generated =
          activeCount > 0 || deleteCount > 0
            ? `ACTIVE x${activeCount}, DELETED x${deleteCount}`
                .replace(/(, )?DELETED x0/, "")
                .replace(/ACTIVE x0(, )?/, "")
            : null;

        if (participant.recurring_history === "15") {
          const createdDate = new Date(participant.created);
          const customTimestamp3 = participant.custom_timestamp_3
            ? new Date(participant.custom_timestamp_3)
            : new Date();

          const comparisonDate = participant.custom_timestamp_3
            ? customTimestamp3
            : new Date();

          const diffInMonths =
            (comparisonDate.getFullYear() - createdDate.getFullYear()) * 12 +
            (comparisonDate.getMonth() - createdDate.getMonth());

          data.affiliated_leads_generated =
            diffInMonths > 0
              ? `${diffInMonths} month${diffInMonths === 1 ? "" : "s"}`
              : null;
        } else {
          data.affiliated_leads_generated = null;
        }

        if (participant.recurring_history === "15" && participant.amount) {
          data.affiliated_money_generated += +participant.amount;
        }

        const results = [];

        if (participant.recurring_history === "14") data.tags += 1;
        if (data.tags > 0) results.push(`Paid x${data.tags}`);

        if (participant.recurring_history === "6") {
          data.petition = results.push("Petition");
        }

        if (participant.recurring_history === "4")
          data.gameTypes.add(participant.game_type);
        if (data.gameTypes.size > 0)
          results.push(...Array.from(data.gameTypes));

        if (participant.coupon_send == "1" && participant.receiver_phone)
          data.giftcards_sent += 1;
        if (data.giftcards_sent > 0)
          results.push(`${data.giftcards_sent} Giftcards sent`);

        if (participant.agree_download_report == "0") {
          data.newsletter = results.push("No newsletter");
        }

        data.all_dates = results.length > 0 ? results.join(", ") : null;
      });

      for (const phone in mergedData) {
        const SwedishTimeZone = new Date().toLocaleString("sv-SE", {
          timeZone: "Europe/Stockholm",
        });

        const data = mergedData[phone];
        data.locations = [...data.locations].join(", ");
        data.participants_id = [...data.participants_id].join(", ");

        const [existingRows] = await poolConnection.query(
          `SELECT * FROM ${db}.leads WHERE phone = ?`,
          [data.phone]
        );

        if (existingRows.length > 0) {
          const existingRow = existingRows[0];
          const hasChanges =
            existingRow.locations !== data.locations ||
            existingRow.participants_id !== data.participants_id ||
            existingRow.total_amount !== data.total_amount ||
            existingRow.giftcards_sent !== data.giftcards_sent ||
            existingRow.name !== data.name ||
            existingRow.email !== data.email ||
            existingRow.address !== data.address ||
            existingRow.zip !== data.postalcode ||
            existingRow.city !== data.city ||
            existingRow.personal_number !== data.personal_number ||
            existingRow.quiz_answers !== data.points_scored ||
            existingRow.custom_field_1 !== data.time_spent ||
            existingRow.custom_field_2 !== data.sms_parts ||
            existingRow.custom_field_3 !== data.sms_cost ||
            existingRow.custom_field_4 !== data.agree_download_report ||
            datacustom_field_5 !== data.custom_field_5 ||
            existingRow.affiliated_views_generated !==
              data.affiliated_views_generated ||
            existingRow.affiliated_leads_generated !==
              data.affiliated_leads_generated ||
            existingRow.affiliated_money_generated !==
              data.affiliated_money_generated ||
            existingRow.tags !== data.tags ||
            existingRow.all_dates !== data.all_dates;
          if (hasChanges) {
            await poolConnection.query(
              `UPDATE ${db}.leads SET
                locations = ?, participants_id = ?, total_amount = ?, giftcards_sent = ?, name = ?, phone = ?, email = ?, address = ?, zip = ?, city = ?, personal_number = ?, quiz_answers = ?, custom_field_1 = ?, custom_field_2 = ?, custom_field_3 = ?, custom_field_4 = ?, custom_field_5 = ?, affiliated_views_generated = ?, affiliated_leads_generated = ?, affiliated_money_generated = ?, tags = ?, all_dates = ?, modified = ?
              WHERE phone = ?`,
              [
                data.locations || null,
                data.participants_id || null,
                data.total_amount || 0,
                data.giftcards_sent || 0,
                data.name || null,
                data.phone || null, // Se till att phone inte är null om det inte är avsett
                data.email || null,
                data.address || null,
                data.zip || null,
                data.city || null,
                data.personal_number || null,
                data.quiz_answers || 0,
                data.custom_field_1 || 0,
                data.custom_field_2 || 0,
                data.custom_field_3 || 0,
                data.custom_field_4 || 0,
                data.custom_field_5 || 0,
                data.affiliated_views_generated || 0,
                data.affiliated_leads_generated || 0,
                data.affiliated_money_generated || 0,
                data.tags || 0,
                data.all_dates || null,
                SwedishTimeZone,
                data.phone, 
              ]
            );

            console.log(`Updated record for phone: ${data.phone}`);
          } else {
            console.log(`No changes for phone: ${data.phone}`);
          }
        } else {
          await poolConnection.query(
            `INSERT INTO ${db}.leads
            (locations, participants_id, total_amount, giftcards_sent, name, phone, email, address, zip, city, personal_number, quiz_answers, custom_field_1, custom_field_2, custom_field_3, custom_field_4, custom_field_5, affiliated_views_generated, affiliated_leads_generated, affiliated_money_generated, tags, all_dates, created)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [
              data.locations || null,
              data.participants_id || null,
              data.total_amount || 0,
              data.giftcards_sent || 0,
              data.name || null,
              data.phone || null,
              data.email || null,
              data.address || null,
              data.zip || null,
              data.city || null,
              data.personal_number || null,
              data.quiz_answers || 0,
              data.custom_field_1 || 0,
              data.custom_field_2 || 0,
              data.custom_field_3 || 0,
              data.custom_field_4 || 0,
              data.custom_field_5 || 0,
              data.affiliated_views_generated || 0,
              data.affiliated_leads_generated || 0,
              data.affiliated_money_generated || 0,
              data.tags || 0,
              data.all_dates || null,
              SwedishTimeZone,
              data.phone, 
            ]
          );
          console.log(`Inserted new record for phone: ${data.phone}`);
        }
      }
    }

    console.log("Data merged successfully!");
  } catch (error) {
    console.error("Error during mergeData:", error);
  }
  console.log("Kör mergeData...");
}
