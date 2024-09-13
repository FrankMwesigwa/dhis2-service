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

import chtRoutes from './routes/cht.js';
import dhis2Routes from './routes/dhis2.js'

app.use(cors());
app.use(express.json());
app.use(cookieParser())

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));

app.use("/api/cht", chtRoutes);
app.use("/api/dhis2", dhis2Routes);

app.listen(process.env.PORT, async () => {
    console.log(`🚀Server started Successfully on port ${process.env.PORT} in ${process.env.NODE_ENV}`);
});
