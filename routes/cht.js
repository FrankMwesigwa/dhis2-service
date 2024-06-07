import express from "express";
import axios from "axios";
import dotenv from "dotenv";

dotenv.config();
const router = express.Router();

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
            //categoryOptionCombo: 'your-categoryOptionCombo-id'
        }))
    };

    try {
        const response = await axios.post(
            `${DHIS2_URL}/dataValueSets`,
            payload,
            { auth }
        );

        res.status(200).json({
            message: 'Data sent successfully to DHIS2',
            data: response.data
        });
    } catch (error) {
        console.error('Error sending data to DHIS2:', error.response ? error.response.data : error.message);
        res.status(500).json({
            message: 'Error sending data to DHIS2',
            error: error.message
        });
    }
});

export default router;
