import express from "express";
import axios from "axios";
import dotenv from "dotenv";
import pkg from 'pg';
import fs from 'fs';
const { Pool } = pkg;

dotenv.config();
const router = express.Router();

const pool = new Pool({
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME
});

const DHIS2_URL = process.env.DHIS2_URL;
const DHIS2_USERNAME = process.env.DHIS2_USERNAME;
const DHIS2_PASSWORD = process.env.DHIS2_PASSWORD;



const auth = {
    username: DHIS2_USERNAME,
    password: DHIS2_PASSWORD
};

async function fetchData() {
    const query = `
    select * from echis_reporting.dhis2_push_cht c where c.period_date = '2024-10-01'
    `;

    const result = await pool.query(query);
    return result.rows;
}

function mapData(rows) {
    return rows.map(row => {
        return {
            orgUnit: row.dhis2_facility_id,
            facility: row.health_facility,
            dataValues: [
                { dataElement: "SKZcAmubSE7", value: row.hh_registered },
                { dataElement: "P7QO6GNlHUe", value: row.hh_people },
                { dataElement: "XbUmjB60oYJ", value: row.vhts_submitted_report },
                { dataElement: "OheQ2BZlvTf", value: row.vhts_expected_to_report },
                { dataElement: "oOFDcl8DThU", value: row.villages_reporting },
                { dataElement: "eHObnrnwW4V", value: row.u1_month_male },
                { dataElement: "mdDR2R9D9To", value: row.u1_month_female },
                { dataElement: "PdCe3hWwi06", value: row.death_u1month_male },
                { dataElement: "BAaGg1ZrHJe", value: row.death_u1month_female },
                { dataElement: "SuAKhTqHqht", value: row.btn_1_11_months_male },
                { dataElement: "Lh51ukePHHM", value: row.btn_1_11_months_female },
                { dataElement: "jhL6I6rrnAW", value: row.death_btn_1_11month_male },
                { dataElement: "oEUie9AeKNk", value: row.death_btn_1_11month_female },
                { dataElement: "IRfz4ZAlfbm", value: row.btn_1_5_yrs_male },
                { dataElement: "iihU4qzzY5L", value: row.btn_1_5_yrs_female },
                { dataElement: "izQ3S19Af6w", value: row.death_btn_1_5yrs_male },
                { dataElement: "k9NsrZNZuVc", value: row.death_btn_1_5yrs_female },
                { dataElement: "ouBxMwA8O70", value: row.u5_yrs_male },
                { dataElement: "JWdLGy7VRej", value: row.u5_yrs_female },
                { dataElement: "Pir53UHbIJx", value: row.btn_5_17_yrs_male },
                { dataElement: "u6LnMOYVSka", value: row.btn_5_17_yrs_female },
                { dataElement: "b96mFugYO6B", value: row.death_btn_5_17yrs_male },
                { dataElement: "fBZbyfZhOQf", value: row.death_btn_5_17yrs_female },
                { dataElement: "iFDSfCDWbPX", value: row.btn_18_24_yrs_male },
                { dataElement: "suGbTLjNthq", value: row.btn_18_24_yrs_female },
                { dataElement: "kmLhZ419RNR", value: row.btn_25_65_yrs_male },
                { dataElement: "mm81NiyEiyr", value: row.btn_25_65_yrs_female },
                { dataElement: "Xw5TqEnXBTG", value: row.death_btn_25_65yrs_male },
                { dataElement: "NsDp5hysAqq", value: row.death_btn_25_65yrs_female },
                { dataElement: "unuaXNqYXQ8", value: row.above_65_yrs_male },
                { dataElement: "N5MYm3hdXne", value: row.above_65_yrs_female },
                { dataElement: "oevEMsVyc7w", value: row.death_over_65yrs_male },
                { dataElement: "W5MBRW6U2ne", value: row.death_over_65yrs_female },
                { dataElement: "pxmRIkFT7Gj", value: row.num_vitamin_a_12_59months_male },
                { dataElement: "dFNzvV49yGY", value: row.num_vitamin_a_12_59months_female },
                { dataElement: "gPWfeqK83Be", value: row.num_dewormed_6_59months_male },
                { dataElement: "yCxuIoo4zeO", value: row.num_dewormed_6_59months_female },
                { dataElement: "cV2HGmPOS5G", value: row.assesed_muac_12_59months_male },
                { dataElement: "jwVM1ck5HO2", value: row.assesed_muac_12_59months_female },
                { dataElement: "bhJ7czojmPK", value: row.yellow_muac_12_59months_male },
                { dataElement: "nAMWBZME9MJ", value: row.yellow_muac_12_59months_female },
                { dataElement: "x6HhIGP1kaM", value: row.red_muac_12_59months_male },
                { dataElement: "SqiLy4Mkjq1", value: row.red_muac_12_59months_female },
                { dataElement: "iw7Sm4Iv0jm", value: row.oedema_12_59months_male },
                { dataElement: "muDxwt6Z7lV", value: row.oedema_12_59months_female },
                { dataElement: "DAes9aO44NR", value: row.referred_with_oedema_yellred_muac_12_59months_male },
                { dataElement: "P8Qevaon3nY", value: row.referred_with_oedema_yellred_muac_12_59months_female },
                { dataElement: "CDAqQZOuzvk", value: row.oedema_yellred_muac_linked_12_59months_male },
                { dataElement: "cBnsyxZ5Wgi", value: row.oedema_yellred_muac_linked_12_59months_female },
                { dataElement: "wPFMFQ8cf0Q", value: row.pregnant_lactating_muac_screened },
                { dataElement: "IMKrac3HiYI", value: row.pregnant_lactating_yellow_muac },
                { dataElement: "tYPe3Yqj0wf", value: row.pregnant_lactating_red_muac },
                { dataElement: "ziXpVYiiZwU", value: row.pregnant_women },
                { dataElement: "puhcsTuqa1P", value: row.num_pregnant_lactating_referred4yellow_red_muac },
                { dataElement: "yWp5NQrcshE", value: row.pregnant_lactating_linked4yellow_red_muac },
                { dataElement: "iMXm9uyilk4", value: row.u5_immunization_upto_date_male },
                { dataElement: "IbltWaQhoGs", value: row.u5_immunization_upto_date_female },
                { dataElement: "GpG8KvU5cYz", value: row.deaf_male },
                { dataElement: "xSrAKs3vU5V", value: row.deaf_female },
                { dataElement: "JbuV5wL2FTt", value: row.lameness_male },
                { dataElement: "KmDXhl0iNG6", value: row.lameness_female },
                { dataElement: "Sdxvqr5LI6g", value: row.difficulty_remembering_male },
                { dataElement: "tV8a8ArUCze", value: row.difficulty_remembering_female },
                { dataElement: "RfqGCEgkpmU", value: row.blindness_male },
                { dataElement: "M9fBRJo22mj", value: row.blindness_female },
                { dataElement: "akFbhlAReXF", value: row.difficulty_communicating_male },
                { dataElement: "UOvmavkj682", value: row.difficulty_communicating_female },
                { dataElement: "sGEqsncqnlk", value: row.difficulty_selfcare_male },
                { dataElement: "nsEuX2oPoiB", value: row.difficulty_selfcare_female },
                { dataElement: "TXsIfbLzqiC", value: row.hiv_positive_male },
                { dataElement: "OoBDCQ1ooFE", value: row.hiv_positive_female },
                { dataElement: "r96GiZ47NcU", value: row.not_on_art_treatment_male },
                { dataElement: "RwbCa72ZhUp", value: row.not_on_art_treatment_female },
                { dataElement: "usfNP3rkTX2", value: row.has_tb_male },
                { dataElement: "U3v1ftmpfg8", value: row.has_tb_female },
                { dataElement: "LeFHn3jEhg2", value: row.not_on_tb_treatment_male },
                { dataElement: "iSk3ch8uEFe", value: row.not_on_tb_treatment_female },
                { dataElement: "Htspk4eFdtq", value: row.using_fp_method_male },
                { dataElement: "FxQEc2KNsUP", value: row.using_fp_method_female },
                { dataElement: "gdQIbHqLkP6", value: row.delivery_at_home },
                { dataElement: "GIzTKtQV2ey", value: row.died_during_pregnancy },
                { dataElement: "hqlAIlGGx0j", value: row.atleast_4_anc_visits },
                { dataElement: "Tj0ofGGRQG4", value: row.atleast_8_anc_visits },
                { dataElement: "OtiHNSdSx8o", value: row.btn_10_24_yrs_male },
                { dataElement: "G77H8sensJO", value: row.btn_10_24_yrs_female },
                { dataElement: "Ob4MIPnzsxM", value: row.adolescents_received_hpv_male },
                { dataElement: "lLuTTTNMMyt", value: row.adolescents_received_hpv_female },
                { dataElement: "nXW6thfcmaj", value: row.adolescents_received_td_male },
                { dataElement: "gs6KSLD8lVL", value: row.adolescents_received_td_female },
                { dataElement: "WuFdPjmRRJb", value: row.sleep_under_llin_male },
                { dataElement: "IGv0JDj3ITb", value: row.sleep_under_llin_female },
                { dataElement: "ohzbQlOQJ0U", value: row.household_have_latrine },
                { dataElement: "hj3zRML6HDR", value: row.household_have_improved_latrine },
                { dataElement: "MHE4UV6dtXZ", value: row.household_functional_handwashing_facility },
                { dataElement: "LKH6L8fqOGm", value: row.num_of_household_safe_water },
                { dataElement: "KsQEOfPFHrr", value: row.household_is_odf },
                { dataElement: "QHM8oVo4BFu", value: row.u5_attended_to_male },
                { dataElement: "QMbYk1fYRaD", value: row.u5_attended_to_female },
                { dataElement: "nG2UvgcdPCs", value: row.u5_diarrhea_male },
                { dataElement: "or2wkB8pXNa", value: row.u5_diarrhea_female },
                { dataElement: "r62xVwT5E1D", value: row.u5_diarrhea_treated_with_ors_male },
                { dataElement: "Ya78umVFGoA", value: row.u5_diarrhea_treated_with_ors_female },
                { dataElement: "kweI4YNvAaK", value: row.u5_diarrhea_treated_24hrs_male },
                { dataElement: "efhJmXOGzqB", value: row.u5_diarrhea_treated_24hrs_female },
                { dataElement: "BdSIrF2gzEP", value: row.u5_fever_male },
                { dataElement: "Ei0zsNp2Nam", value: row.u5_fever_female },
                { dataElement: "MRHScUBXqm8", value: row.u5_with_fever_received_mrdt_male },
                { dataElement: "VPDyF2ZtKFg", value: row.u5_with_fever_received_mrdt_female },
                { dataElement: "mqRtGgRHbUZ", value: row.u5_mrdt_positive_male },
                { dataElement: "EQOP7ZjBS7Z", value: row.u5_mrdt_positive_female },
                { dataElement: "t8Gd6nigCfc", value: row.u5_malaria_received_act_male },
                { dataElement: "HrVoQsKNPN6", value: row.u5_malaria_received_act_female },
                { dataElement: "MiynY0Cutnx", value: row.u5_fever_and_danger_signs_male },
                { dataElement: "wytMu2X4Ksh", value: row.u5_fever_and_danger_signs_female },
                { dataElement: "jO0q79HlysG", value: row.u5_fever_and_danger_sign_treated_with_rectal_male },
                { dataElement: "gjZZXoj5mCC", value: row.u5_fever_and_danger_sign_treated_with_rectal_female },
                { dataElement: "Py6r3XejF9e", value: row.u5_fever_treated_24hours_male },
                { dataElement: "ruAcUSww49Y", value: row.u5_fever_treated_24hours_female },
                { dataElement: "UpicecBTlgH", value: row.u5_with_pneumonia_male },
                { dataElement: "TRXsk12FeVW", value: row.u5_with_pneumonia_female },
                { dataElement: "GyLFBoBmTae", value: row.u5_pneumonia_received_amoxicillin_male },
                { dataElement: "pRVh8PNz8K6", value: row.u5_pneumonia_received_amoxicillin_female },
                { dataElement: "F3w1gDysXH2", value: row.u5_pneumonia_24hr_treatment_male },
                { dataElement: "uYFpOeomsLq", value: row.u5_pneumonia_24hr_treatment_female },
                { dataElement: "Sb82lwWeHms", value: row.u5_managed_recovered_by_vht_male },
                { dataElement: "ehF8z9Teod7", value: row.u5_managed_recovered_by_vht_female },
                { dataElement: "Y9MUfblgjWw", value: row.hiv_exposure_male },
                { dataElement: "HoKKQk5siCu", value: row.hiv_exposure_female },
                { dataElement: "IbCpXXVN2QK", value: row.suspected_tb_male },
                { dataElement: "zRBCRv4exyX", value: row.suspected_tb_female },
                { dataElement: "VETDzokpH27", value: row.pnc_atleast_2visits_male },
                { dataElement: "Of8EZDbhPN5", value: row.pnc_atleast_2visits_female },
                { dataElement: "WgRvzNrpu61", value: row.sick_reffered_male },
                { dataElement: "JkrAQg2cVc7", value: row.sick_reffered_female },
                { dataElement: "wozPliVW6dd", value: row.act_item_received },
                { dataElement: "BKbaOBwOYOX", value: row.act_item_returned },
                { dataElement: "dsjxSlajJhL", value: row.act_given_iccm },
                { dataElement: "cJm2qpsmczU", value: row.zinc_item_received },
                { dataElement: "iWFtEdtIIiU", value: row.zinc_item_returned },
                { dataElement: "FRuEGyAgHfU", value: row.zinc_given_iccm },
                { dataElement: "s5FtDCNBH8y", value: row.amoxicillin_item_received },
                { dataElement: "GQnehOCvC4J", value: row.amoxicillin_item_returned },
                { dataElement: "RRTKR5IlJDs", value: row.amoxicillin_given_iccm },
                { dataElement: "fmPudbY1m2x", value: row.malaria_rdts_item_received },
                { dataElement: "xTYFX0sgcMA", value: row.malaria_rdts_item_returned },
                { dataElement: "Xodl5kTw4hA", value: row.malaria_rdts_given_iccm },
                { dataElement: "MFpTW2tWRDF", value: row.pop_item_received },
                { dataElement: "zJ5BUsILBQ0", value: row.pop_item_returned },
                { dataElement: "d5jtouKiQEN", value: row.pop_given_fp },
                { dataElement: "PWOz0aBzm4X", value: row.dmpa_item_received },
                { dataElement: "POwO2W7fOtZ", value: row.dmpa_item_returned },
                { dataElement: "IPLqqCPOkKh", value: row.dmpa_given_fp },
                { dataElement: "OymDJGBxuQU", value: row.coc_item_received },
                { dataElement: "XzcDMaMCcl2", value: row.coc_item_returned },
                { dataElement: "LKVzqOaiEFd", value: row.coc_given_fp },
                { dataElement: "Xd7puw6f1Zj", value: row.condoms_item_received },
                { dataElement: "Ams90EdAZDx", value: row.condoms_item_returned },
                { dataElement: "HaZWptEpQSl", value: row.condoms_given_fp },
                { dataElement: "EyPkT8vx4zY", value: row.contraceptives_item_received },
                { dataElement: "Fv2fAh5hiu4", value: row.contraceptives_item_returned },
                { dataElement: "wOXmothpahc", value: row.contraceptives_given_fp },
                { dataElement: "lrz2dXXBRGd", value: row.rectal_item_received },
                { dataElement: "eFQIRqX6FdA", value: row.rectal_item_returned },
                { dataElement: "OLAhtZdD3fE", value: row.rectal_given_iccm },
                { dataElement: "oNuPNEQjtdx", value: row.gloves_item_received },
                { dataElement: "O9iuFMT2WGD", value: row.gloves_item_returned },
                { dataElement: "NNtkdxluTZ9", value: row.gloves_given_iccm },
            ]
        };
    });
}

async function pushData(mappedData) {
    const successLogStream = fs.createWriteStream('success_log.txt', { flags: 'a' });
    try {
        const payloads = mappedData.map(data => {
            return {
                dataSet: "DNYmDmBxfGG",
                // completeDate: "2024-01-31",
                period: "2024010",
                orgUnit: data.orgUnit,
                dataValues: data.dataValues
            };
        });

        for (const payload of payloads) {
            const response = await axios.post(
                `${DHIS2_URL}/dataValueSets`,
                payload,
                { auth },
            );
            console.log(`Data sent successfully to DHIS2 for facility ID ${payload.orgUnit}, Facility: ${mappedData.find(d => d.orgUnit === payload.orgUnit).facility}`);
            successLogStream.write(`Data sent successfully to DHIS2 for facility ID ${payload.orgUnit}, Facility: ${mappedData.find(d => d.orgUnit === payload.orgUnit).facility}\n`);
        }
    } catch (error) {
        console.error('Error sending data to DHIS2:', error.response ? error.response.data : error.message);
        throw error;
    } finally {
        successLogStream.end();
    }
}

router.post('/', async (req, res) => {
    try {
        const data = await fetchData();
        const mappedData = mapData(data);
        await pushData(mappedData);

        res.status(200).json({
            message: 'Data sent successfully to DHIS2'
        });
    } catch (error) {
        res.status(500).json({
            message: 'Error sending data to DHIS2',
            error: error.response ? error.response.data : error.message
        });
    }
});

export default router;