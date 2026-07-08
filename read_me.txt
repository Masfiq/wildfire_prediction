Wildfire Risk Prediction — Research Plan
Core Idea
Predict where fire will occur in the next 30 days in Western USA using multi-source satellite + environmental data. The novelty for IGARSS/IEEE TGRS: using HLS at 30m resolution (much finer than most prior work at 500m–1km) combined with weather and topography, with MODIS burned area as ground truth labels. We will also try to do the burned area mapping with the timeseries where the same ground truth data can be used 



Phase 1 — Data Collection (Weeks 1–4)
Data	Product	Resolution	Purpose
Burned area (labels)	MODIS MCD64A1	500m, monthly	Ground truth — where fire happened
Vegetation/spectral	HLS S30/L30	30m	Pre-fire vegetation state (NDVI, NBR, EVI)
Weather	GRIDMET	4km, daily	Temp, humidity, wind, drought index (PDSI)
Topography	NASADEM / SRTM	30m	Elevation, slope, aspect
Fuel / land cover	LANDFIRE or NLCD	30m	Fuel type, land use
Study area: California + Oregon + Washington, 2018–2023 (covers major fire years like 2020–2021).







































Phase 2 — Preprocessing & Label Creation (Weeks 3–6)
Reproject everything to common 30m grid (UTM)
For each fire event in MODIS MCD64A1: extract the 30–90 day pre-fire window of HLS + weather data as input features
Create binary labels: burned pixel = 1, matched unburned control pixels = 0
Handle class imbalance (burned pixels are rare) — use stratified sampling or weighted loss
Phase 3 — Feature Engineering (Weeks 5–8)
Key input features:

Vegetation dryness: NBR, NDVI, EVI time series (trend over 30–90 days pre-fire)
Weather: max temp, min humidity, wind speed, consecutive dry days, PDSI drought index
Topography: elevation, slope, aspect, terrain ruggedness
Fuel: land cover class, canopy cover
Phase 4 — Model Development (Weeks 7–14)
Baseline: Random Forest (tabular features per pixel) — easy to interpret, good for TGRS
Main model: U-Net or CNN taking spatiotemporal HLS patches as input — captures spatial context
Ablation: Weather only vs. HLS only vs. combined → shows contribution of each data source
Phase 5 — Evaluation (Weeks 13–16)
Metrics: AUC-ROC, F1, IoU (spatial overlap), precision/recall
Spatial cross-validation (leave-one-region-out to avoid data leakage)
Temporal holdout: train on 2018–2021, test on 2022–2023
Compare against existing baselines (USFS fire danger rating, prior MODIS-based models)
Phase 6 — Paper Writing (Weeks 15–20)
Target: IGARSS 2027 (abstract deadline typically ~Jan) or IEEE TGRS (journal, rolling submission)

Paper sections:

Introduction + motivation
Study area + datasets
Methodology (data pipeline + model)
Results + ablation
Discussion (limitations, future work)
Immediate Next Steps
Set up NASA Earthdata account → download MODIS MCD64A1 for California 2018–2023
Download matching HLS tiles for same region/period
Write preprocessing scripts to co-register everything to 30m grid
Exploratory analysis: visualize a few fire events with pre-fire imagery
Want to start with the data download scripts?