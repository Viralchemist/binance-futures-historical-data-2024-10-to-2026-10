# مصادر وملاحظات التحقق

تم التحقق في 2026-10-25 من مصادر Binance الرسمية التالية:

1. مستودع Binance Public Data يصف أرشيفات يومية وشهرية، ويذكر أن بيانات USD-M klines مصدرها `/fapi/v1/klines`، كما يوثق ترتيب الحقول ومخطط SHA256 بجانب أرشيف ZIP: https://github.com/binance/binance-public-data
2. فهرس أرشيف البيانات: https://data.binance.vision/data/futures/um/
3. توثيق Binance USDⓈ-M Futures Market Data: https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data
4. واجهة معلومات العقود الحالية: https://fapi.binance.com/fapi/v1/exchangeInfo
5. واجهة سجل التمويل الرسمية، وتشمل `fundingRate`, `fundingTime`, و`markPrice`: https://fapi.binance.com/fapi/v1/fundingRate

مسارات عينات الأرشيف التي أعادت HTTP 200:

- `data/futures/um/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-11.zip` — 1,876,449 bytes.
- المساران المقابلان لـ5m و1h للشهر نفسه، وأعادا HTTP 200.
- `data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2024-11.zip` — 746 bytes.
- `data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2026-10-18.zip` — 59,759 bytes.
- ملف `.CHECKSUM` للعينة الشهرية أعاد SHA256 صالحًا وتم التحقق منه مقابل الأرشيف عند التنفيذ.

جميع الرموز العشرين المطلوبة ظهرت في `exchangeInfo` وقت التحقق بوصفها `PERPETUAL` مع `quoteAsset=USDT`. سيتم حفظ بيانات التحقق و`onboardDate` داخل المشروع، والتحقق أيضًا من أول شمعة فعلية ضمن الفترة.

أرشيف Aggregate Trades لعقد BTCUSDT لشهر 2025-01 أعاد حجمًا معلنًا قدره 692,363,439 bytes للملف المضغوط. لم تُنزّل صفقات aggregate؛ تعميم هذا الحجم على كامل الفترة والعملات سيكون كبيرًا وغير عملي مقارنة بالأولوية المطلوبة للشموع والتمويل. هذا تقدير استدلالي من ملف عينة واحد، لا حجم إجمالي مضمون.

سيكون مصدر الأسعار والأحجام هو أرشيف USDⓈ-M Futures الرسمي، وليس Spot أو COIN-M. وتُستخدم واجهة USDⓈ-M الرسمية للتمويل ومعلومات العقود. لا تُولَّد شموع بديلة ولا تُملأ الفجوات بالاستيفاء.
