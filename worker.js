import { MongoClient } from 'mongodb';
import yahooFinance from 'yahoo-finance2';
import PQueue from 'p-queue';
import dotenv from 'dotenv';

dotenv.config();

// Configuration
const MONGO_URI = process.env.MONGODB_URI;
const MONGO_DB_NAME = "nse_bse_stocks";
const MONGO_COLLECTION_NAME = "eod_data_1";
const CONCURRENT_REQUESTS = 1;
const REQUEST_INTERVAL_MS = 1000;
const REQUESTS_PER_INTERVAL = 1;

async function updateEodData() {
    if (!MONGO_URI) {
        console.error('MongoDB URI is not defined.');
        return;
    }

    const client = new MongoClient(MONGO_URI);

    try {
        await client.connect();
        const db = client.db(MONGO_DB_NAME);
        const collection = db.collection(MONGO_COLLECTION_NAME);
        console.log('Connected to MongoDB.');

        const stocks = await collection.find({}, { projection: { symbol: 1, _id: 0 } }).toArray();
        if (!stocks.length) {
            console.log('No stocks found.');
            return;
        }
        console.log(`Processing ${stocks.length} stocks...`);

        const queue = new PQueue({
            concurrency: CONCURRENT_REQUESTS,
            interval: REQUEST_INTERVAL_MS,
            intervalCap: REQUESTS_PER_INTERVAL,
        });

        let successCount = 0, noDataCount = 0, errorCount = 0, processedCount = 0;

        const today = new Date();
        const tomorrow = new Date(today);
        tomorrow.setDate(tomorrow.getDate() + 1);

        const period1 = today.toISOString().split('T')[0];
        const period2 = tomorrow.toISOString().split('T')[0];

        stocks.forEach(stock => {
            queue.add(async () => {
                try {
                    const result = await yahooFinance.chart(stock.symbol, { period1, period2, interval: '1d' });
                    const latestQuote = result.quotes[0];

                    if (latestQuote?.date) {
                        const newEodRecord = {
                            date: new Date(latestQuote.date),
                            open: parseFloat(latestQuote.open) || null,
                            high: parseFloat(latestQuote.high) || null,
                            low: parseFloat(latestQuote.low) || null,
                            close: parseFloat(latestQuote.close) || null,
                            adjClose: parseFloat(latestQuote.adjClose) || null,
                            volume: parseInt(latestQuote.volume, 10) || null,
                        };

                        const updateResult = await collection.updateOne(
                            { symbol: stock.symbol, 'eods.date': { $ne: newEodRecord.date } },
                            { $push: { eods: { $each: [newEodRecord], $position: 0, $sort: { date: -1 } } } }
                        );
                        if (updateResult.modifiedCount > 0) successCount++; else noDataCount++;
                    } else {
                        noDataCount++;
                    }
                } catch (error) {
                    errorCount++;
                    console.error(`Error for ${stock.symbol}: ${error.message}`);
                } finally {
                    processedCount++;
                }
            });
        });

        await queue.onIdle();
        console.log(`Done. Processed: ${processedCount}, Updated: ${successCount}, No New Data: ${noDataCount}, Errors: ${errorCount}`);
    } catch (err) {
        console.error('Unexpected error:', err);
    } finally {
        await client.close();
    }
}

updateEodData();
