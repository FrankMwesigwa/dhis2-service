import express from 'express';
import cors from "cors";
import dotenv from "dotenv";
import morgan from "morgan";
import bodyParser from 'body-parser';
import cookieParser from 'cookie-parser';

const app = express()
dotenv.config();

app.use(express.json({ limit: "10kb" }));
if (process.env.NODE_ENV === "development") app.use(morgan("dev"));

import dhis from './routes/dhis.js';
import opensrp from './routes/opensrp.js'
import cht from './routes/cht.js'

app.use(cors());
app.use(express.json());
app.use(cookieParser())

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));

app.use("/api/cht", cht);
app.use("/api/dhis", dhis);
app.use("/api/opensrp", opensrp);

app.listen(process.env.PORT, async () => {
    console.log(`🚀Server started Successfully on port ${process.env.PORT} in ${process.env.NODE_ENV}`);
});
