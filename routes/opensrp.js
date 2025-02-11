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
    select * FROM echis_reporting.dhis2_push WHERE district = 'Kamwenge District' AND dhis2_facility_id is not null AND month = 10
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
                { dataElement: "r62xVwT5E1D",value: row.male_children_diarrhea_ors},
                { dataElement: "Ya78umVFGoA",value: row.female_children_diarrhea_ors},
                { dataElement: "kweI4YNvAaK",value: row.male_children_diarrhea},
                { dataElement: "efhJmXOGzqB",value: row.female_children_diarrhea},
                { dataElement: "BdSIrF2gzEP",value: row.male_children_2_5_fever},
                { dataElement: "Ei0zsNp2Nam",value: row.female_children_2_5_fever},
                { dataElement: "mqRtGgRHbUZ",value: row.m_children_2_5_malaria_rdt},
                // { dataElement: "EQOP7ZjBS7Z",value: row.f_children_2_5_malaria_rdt},
                { dataElement: "t8Gd6nigCfc",value: row.m_children_2_5_malaria_act},
                { dataElement: "HrVoQsKNPN6",value: row.f_children_2_5_malaria_act},
                { dataElement: "MiynY0Cutnx",value: row.m_children_2_5_fever_danger_signs},
                { dataElement: "wytMu2X4Ksh",value: row.f_children_2_5_fever_danger_signs},
                { dataElement: "jO0q79HlysG",value: row.m_children_2_5_fever_danger_signs_rectal},
                { dataElement: "gjZZXoj5mCC",value: row.f_children_2_5_fever_danger_signs_rectal},
                { dataElement: "Py6r3XejF9e",value: row.male_children_2_5_fever_treated},
                { dataElement: "ruAcUSww49Y",value: row.female_children_2_5_fever_treated},
                // { dataElement: "UpicecBTlgH",value: row.m_children_2_5_pneumonia_fast_breathing},
                // { dataElement: "TRXsk12FeVW",value: row.f_children_2_5_pneumonia_fast_breathing},
                { dataElement: "GyLFBoBmTae",value: row.m_children_2_5_pneu_amoxicillin},
                { dataElement: "pRVh8PNz8K6",value: row.f_children_2_5_pneu_amoxicillin},
                { dataElement: "F3w1gDysXH2",value: row.m_children_2_5_treated_pneumonia},
                { dataElement: "uYFpOeomsLq",value: row.f_children_2_5_treated_pneumonia},
                // { dataElement: "SKZcAmubSE7",value: row.households_registered},
                { dataElement: "eHObnrnwW4V",value: row.male_under_1_month},
                { dataElement: "mdDR2R9D9To",value: row.female_under_1_month},
                { dataElement: "PdCe3hWwi06",value: row.male_children_deceased},
                { dataElement: "BAaGg1ZrHJe",value: row.female_children_deceased},
                { dataElement: "SuAKhTqHqht",value: row.male_children_1_11_months},
                { dataElement: "Lh51ukePHHM",value: row.female_children_1_11_months},
                { dataElement: "jhL6I6rrnAW",value: row.male_children_1_11_deceased},
                { dataElement: "oEUie9AeKNk",value: row.female_children_1_11_deceased},
                { dataElement: "IRfz4ZAlfbm",value: row.male_children_1_5_yrs},
                { dataElement: "iihU4qzzY5L",value: row.female_children_1_5_yrs},
                { dataElement: "izQ3S19Af6w",value: row.male_children_1_5_deceased},
                { dataElement: "k9NsrZNZuVc",value: row.female_children_1_5_deceased},
                { dataElement: "ouBxMwA8O70",value: row.male_children_under_5_yrs},
                { dataElement: "JWdLGy7VRej",value: row.female_children_under_5_yrs},
                { dataElement: "Pir53UHbIJx",value: row.male_5_17_yrs},
                { dataElement: "u6LnMOYVSka",value: row.female_5_17_yrs},
                { dataElement: "b96mFugYO6B",value: row.female_5_17_yrs_died},
                { dataElement: "fBZbyfZhOQf",value: row.male_5_17_yrs_died},
                { dataElement: "iFDSfCDWbPX",value: row.male_18_24_yrs},
                { dataElement: "suGbTLjNthq",value: row.female_18_24_yrs},
                { dataElement: "kmLhZ419RNR",value: row.male_25_65_yrs},
                { dataElement: "mm81NiyEiyr",value: row.female_25_65_yrs},
                { dataElement: "Xw5TqEnXBTG",value: row.male_25_65_yrs_died},
                { dataElement: "NsDp5hysAqq",value: row.female_25_65_yrs_died},
                { dataElement: "unuaXNqYXQ8",value: row.male_above_66_yrs},
                { dataElement: "N5MYm3hdXne",value: row.female_above_66_yrs},
                { dataElement: "oevEMsVyc7w",value: row.male_above_66_yrs_died},
                { dataElement: "W5MBRW6U2ne",value: row.female_above_66_yrs_died},
                { dataElement: "ohzbQlOQJ0U",value: row.households_with_latrine},
                { dataElement: "hj3zRML6HDR",value: row.households_with_improved_latrine},
                { dataElement: "MHE4UV6dtXZ",value: row.households_with_handwashing},
                { dataElement: "LKH6L8fqOGm",value: row.households_with_drinking_water},
                { dataElement: "KsQEOfPFHrr",value: row.households_is_odf},
                // { dataElement: "OheQ2BZlvTf",value: row.villages_expected_reporting},
                { dataElement: "XbUmjB60oYJ",value: row.vhts_reporting},
                { dataElement: "oOFDcl8DThU",value: row.villages_reporting},
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