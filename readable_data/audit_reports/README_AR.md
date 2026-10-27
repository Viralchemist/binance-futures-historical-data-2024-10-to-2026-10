# Binance USDⓈ-M Futures Historical Data Audit

## الفترة والنطاق

الفترة المطلوبة: `2024-10-19T00:00:00+00:00` شاملة حتى `2026-10-19T00:00:00+00:00` غير شاملة، أي 730 يومًا. المنطقة الزمنية لجميع الطوابع UTC. البيانات مخصصة للتحقق التاريخي فقط؛ لم تُطوّر أو تُعدّل أي استراتيجية في هذه المهمة.

الرموز: BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT, ADAUSDT, DOGEUSDT, LINKUSDT, LTCUSDT, DOTUSDT, AVAXUSDT, TRXUSDT, BCHUSDT, NEARUSDT, APTUSDT, ARBUSDT, OPUSDT, SUIUSDT, INJUSDT, FILUSDT. جرى التحقق من `/fapi/v1/exchangeInfo`، ونوع العقود الحالية perpetual وتسويتها USDT، كما حُفظ `onboardDate` لكل رمز [2] [4]. فحص تغطية الشموع الفعلية هو المرجع النهائي لأي نقص.

## المصادر والحقول

شموع 1m و5m و1h مأخوذة من ملفات Binance Vision تحت `data/futures/um`, وهي أرشيفات USDⓈ-M Futures وليست Spot أو COIN-M [1] [3]. احتُفظ بترتيب حقول المصدر الأصلي وأضيف صف أسماء الحقول الذي تعرضه ملفات Binance [1]. الطوابع الأصلية Unix milliseconds UTC؛ الأسعار والأحجام لم تُحوّل أو تُعدّل. الملفات الشهرية بصيغة `.csv.gz` داخل أرشيفات ZIP.

حقول شموع المصدر: `open_time, open, high, low, close, volume, close_time, quote_volume, count, taker_buy_volume, taker_buy_quote_volume, ignore`. الحجم الأساسي بوحدة الأصل، و`quote_volume` و` taker_buy_quote_volume` بوحدة USDT بحسب ملف العقد.

التمويل مأخوذ من Binance USD-M Futures `/fapi/v1/fundingRate` متضمنًا `fundingRate`, `fundingTime`, و`markPrice` عندما يرسله المصدر [2] [5]. قورن المعدل وتوقيت السجل بأرشيف Binance Vision الشهري الذي يضم وقت الحساب وفاصل التمويل [1] [3]. المقارنة الأرشيفية تمت حتى 2026-09؛ سجلات أكتوبر 2026 الجزئية مصدرها API فقط. لم تُسند قيمة صفر إلى سجل مفقود، ولم يُستوفَ أي نقص.

## نتائج التغطية وجودة البيانات

ملخص الشموع بحسب الإطار:
- `1h`: 20 رموز، 350,400 صفًا؛ المفقود المتوقع 0، المكرر 0، OHLC غير صالح 0، خلايا ناقصة/غير رقمية 0.
- `1m`: 20 رموز، 21,024,000 صفًا؛ المفقود المتوقع 0، المكرر 0، OHLC غير صالح 0، خلايا ناقصة/غير رقمية 0.
- `5m`: 20 رموز، 4,204,800 صفًا؛ المفقود المتوقع 0، المكرر 0، OHLC غير صالح 0، خلايا ناقصة/غير رقمية 0.

التمويل: 43,800 سجلًا عبر 20 رموز؛ قيم mark price المفقودة 0؛ فجوات تمويل محتملة بحسب الفاصل التاريخي 0؛ اختلافات المعدل عن أرشيف Vision 0. افحص التقرير لكل رمز قبل الاستخدام.

مطابقة 1h مع تجميع 1m: 350,400 ساعات كاملة قابلة للمقارنة؛ 0 ساعات لم يتوفر لها 60 دقيقة فريدة؛ 80 ساعات بها اختلاف واحد أو أكثر في الحقول المقاسة، عبر 80 رمز/ساعة فريدة. راجع `hourly_validation_report.csv` لأقصى الفروق و`hourly_validation_mismatch_detail.csv` لسجل كل فرق. لا تُعامل هذه الفروق كضوضاء رقمية: قيم التداول/الحجم تختلف في بعض الساعات، ولم تُسوَّ البيانات بين المصدرين.
أكبر فروق مطلقة بين المصدرين: open=83.9 (BTCUSDT); high=30.2 (BTCUSDT); low=109.8 (BTCUSDT); volume=28,878,586 (DOGEUSDT); quote_volume=145,879,892.424 (BTCUSDT); count=28,874 (ETHUSDT). volume بوحدة الأصل للرمز المشار إليه، وquote_volume بوحدة USDT؛ هذه فروق في الساعات المتباينة وليست فروق تقريب عامة.
فحص إضافي محدود عبر Klines REST الرسمية على 4 رموز × 5 ساعات: أُعيدت 60 شمعة 1m وشمعة 1h لكل عينة؛ 4 من 20 عينة ظل فيها تجميع REST لـ1m مختلفًا عن شمعة REST لـ1h. التفاصيل وروابط API في `hourly_validation_api_spotcheck.csv`؛ لا تدّعي هذه العينة حسم سبب الاختلافات أو إصلاحها.

الملفات الأخيرة ضمن الفترة مغلقة بالكامل لأن آخر وقت فتح مطلوب يسبق النهاية الحصرية. يفحص `data_quality_report.csv` توافق وقت الإغلاق وبيانات الشموع الأخيرة. الفجوات تُسجّل ولا تُملأ.

## استخدام الملفات

فك ضغط أرشيف الإطار المطلوب، ثم فك ضغط ملفات `.csv.gz` داخله. يمكن قراءتها في Python بـ `pandas.read_csv("BTCUSDT_1m_2024-10.csv.gz")`. لا تغيّر أسماء الأعمدة إذا أردت إعادة المقارنة مع أرشيف Binance.

ملفات التدقيق: `data_manifest.csv`, `data_quality_report.csv`, `funding_quality_report.csv`, `hourly_validation_report.csv`, `hourly_validation_mismatch_detail.csv`, `hourly_validation_api_spotcheck.csv`, `source_download_log.csv`, `symbol_listing_check.csv`, و`SHA256SUMS.txt`. يسجل `data_manifest.csv` الفترة المتوقعة والفعلية، عدد الصفوف، الحجم، المصدر وحالة التحقق لكل ملف.

## قيود وما لم يُنزّل

لم تُنزّل Aggregate Trades التفصيلية. حجم ملف Binance الرسمي لعينة BTCUSDT يناير 2025 كان 692,363,439 bytes مضغوطًا [1]؛ تعميم الجمع على 20 عقدًا وسنتين غير عملي هنا. هذا قياس عينة، وليس تقديرًا دقيقًا لحجم كامل البيانات. لا تتضمن الحزمة بيانات اصطناعية ولا Spot ولا COIN-M.

## أرشيفات التسليم

- `BINANCE_FUTURES_1m_2024-10_to_2026-10.zip` — 789,611,084 bytes.
- `BINANCE_FUTURES_5m_2024-10_to_2026-10.zip` — 172,259,859 bytes.
- `BINANCE_FUTURES_1h_2024-10_to_2026-10.zip` — 16,609,407 bytes.
- `BINANCE_FUTURES_FUNDING_2024-10_to_2026-10.zip` — 874,356 bytes.
- `BINANCE_FUTURES_AUDIT_REPORTS.zip` — حزمة تقارير وبيانات وصفية.

## المراجع الرسمية

[1]: https://github.com/binance/binance-public-data "Binance Public Data README"

[2]: https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data "Binance USD-M Futures Market Data API"

[3]: https://data.binance.vision/data/futures/um/ "Binance Data Vision USD-M archive"

[4]: https://fapi.binance.com/fapi/v1/exchangeInfo "Binance Exchange Information"

[5]: https://fapi.binance.com/fapi/v1/fundingRate "Binance Funding Rate History"
