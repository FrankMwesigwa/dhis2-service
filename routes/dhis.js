import express from "express";
import axios from "axios";
import dotenv from "dotenv";
import pkg from 'pg';
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

router.post('/', async (req, res) => {
    const { dataset, completeDate, period, orgUnit, dataValues } = req.body;

    // Construct payload for DHIS2 API
    const payload = {
        dataSet: dataset,
        completeDate: completeDate,
        period: period,
        orgUnit: orgUnit,
        dataValues: dataValues.map(dataValue => ({
            ...dataValue,
        }))
    };

    try {
        const response = await axios.post(
            `${DHIS2_URL}/dataValueSets`,
            payload,
            { auth },
        );

        res.status(200).json({
            message: 'Data sent successfully to DHIS2',
            data: response.data
        });
    } catch (error) {
        console.error('Error sending data to DHIS2:', error.response ? error.response.data : error.message);
        res.status(500).json({
            message: 'Error sending data to DHIS2',
            error: error.response.data
        });
    }
});

router.post('/facility', async (req, res) => {

    try {
        const result = await pool.query(`
            SELECT dataset, period, orgunit, dataelement, value
            FROM steward.data_values
        `);

        // Group the data by dataset, period, and orgUnit
        const groupedData = {};
        result.rows.forEach(row => {
            const key = `${row.dataset}-${row.period}-${row.orgunit}`;
            if (!groupedData[key]) {
                groupedData[key] = {
                    dataSet: row.dataset,
                    period: row.period,
                    orgUnit: row.orgunit,
                    dataValues: []
                };
            }
            groupedData[key].dataValues.push({
                dataElement: row.dataelement,
                value: row.value
            });
        });

        // Send data to DHIS2 for each group
        for (const key in groupedData) {
            const payload = {
                ...groupedData[key],
                completeDate: new Date().toISOString().split('T')[0]
            };

            try {
                const response = await axios.post(
                    `${DHIS2_URL}/dataValueSets`,
                    payload,
                    { auth }
                );

                console.log(`Data sent successfully for ${key}:`, response.data);
            } catch (error) {
                console.error(`Error sending data for ${key}:`, error.response ? error.response.data : error.message);
                res.status(500).json({
                    message: `Error sending data for ${key}`,
                    error: error.response.data
                });
                return; // Exit on first error
            }
        }

        res.status(200).json({
            message: 'All data sent successfully to DHIS2'
        });
    } catch (error) {
        console.error('Error querying the database:', error.message);
        res.status(500).json({
            message: 'Error querying the database',
            error: error.response.data
        });
    }
});

export default router;
