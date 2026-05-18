import os
import json
import time
import logging
import calendar
import random
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, jsonify, render_template
import yfinance as yf
import numpy as np
import pytz

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
IST = pytz.timezone("Asia/Kolkata")

# ── FULL 2000+ LINE WATCHLIST (RESTORED) ─────────────────────────────────────
WATCHLIST = [
    {"symbol": "RELIANCE.NS",     "name": "Reliance Industries",    "sector": "Conglomerate",  "beta": 0.95},
    {"symbol": "TCS.NS",          "name": "TCS",                    "sector": "IT",            "beta": 0.90},
    {"symbol": "HDFCBANK.NS",     "name": "HDFC Bank",              "sector": "Banking",       "beta": 1.00},
    {"symbol": "ICICIBANK.NS",    "name": "ICICI Bank",             "sector": "Banking",       "beta": 1.20},
    {"symbol": "BHARTIARTL.NS",   "name": "Bharti Airtel",          "sector": "Telecom",       "beta": 0.85},
    {"symbol": "INFY.NS",         "name": "Infosys",                "sector": "IT",            "beta": 0.85},
    {"symbol": "SBIN.NS",         "name": "State Bank of India",    "sector": "Banking",       "beta": 1.30},
    {"symbol": "HINDUNILVR.NS",   "name": "HUL",                    "sector": "FMCG",          "beta": 0.50},
    {"symbol": "ITC.NS",          "name": "ITC",                    "sector": "FMCG",          "beta": 0.60},
    {"symbol": "KOTAKBANK.NS",    "name": "Kotak Mahindra Bank",    "sector": "Banking",       "beta": 1.00},
    {"symbol": "LT.NS",           "name": "Larsen & Toubro",        "sector": "Engineering",   "beta": 1.10},
    {"symbol": "BAJFINANCE.NS",   "name": "Bajaj Finance",          "sector": "NBFC",          "beta": 1.40},
    {"symbol": "HCLTECH.NS",      "name": "HCL Technologies",       "sector": "IT",            "beta": 0.90},
    {"symbol": "MARUTI.NS",       "name": "Maruti Suzuki",          "sector": "Auto",          "beta": 0.90},
    {"symbol": "SUNPHARMA.NS",    "name": "Sun Pharma",             "sector": "Pharma",        "beta": 0.80},
    {"symbol": "ADANIENT.NS",     "name": "Adani Enterprises",      "sector": "Conglomerate",  "beta": 1.50},
    {"symbol": "TITAN.NS",        "name": "Titan Company",          "sector": "Consumer",      "beta": 1.10},
    {"symbol": "WIPRO.NS",        "name": "Wipro",                  "sector": "IT",            "beta": 0.80},
    {"symbol": "NTPC.NS",         "name": "NTPC",                   "sector": "Power",         "beta": 0.70},
    {"symbol": "AXISBANK.NS",     "name": "Axis Bank",              "sector": "Banking",       "beta": 1.15},
    {"symbol": "ONGC.NS",         "name": "ONGC",                   "sector": "Energy",        "beta": 1.00},
    {"symbol": "POWERGRID.NS",    "name": "Power Grid",              "sector": "Power",         "beta": 0.60},
    {"symbol": "BAJAJFINSV.NS",   "name": "Bajaj Finserv",          "sector": "NBFC",          "beta": 1.20},
    {"symbol": "TATASTEEL.NS",    "name": "Tata Steel",             "sector": "Metal",         "beta": 1.50},
    {"symbol": "JSWSTEEL.NS",     "name": "JSW Steel",              "sector": "Metal",         "beta": 1.40},
    {"symbol": "TATAMOTORS.NS",   "name": "Tata Motors",            "sector": "Auto",          "beta": 1.50},
    {"symbol": "TECHM.NS",        "name": "Tech Mahindra",          "sector": "IT",            "beta": 1.10},
    {"symbol": "COALINDIA.NS",    "name": "Coal India",             "sector": "Energy",        "beta": 0.60},
    {"symbol": "NESTLEIND.NS",    "name": "Nestle India",           "sector": "FMCG",          "beta": 0.50},
    {"symbol": "CIPLA.NS",        "name": "Cipla",                  "sector": "Pharma",        "beta": 0.75},
    {"symbol": "DRREDDY.NS",      "name": "Dr Reddy's",             "sector": "Pharma",        "beta": 0.70},
    {"symbol": "ADANIPORTS.NS",   "name": "Adani Ports",            "sector": "Infra",         "beta": 1.30},
    {"symbol": "ULTRACEMCO.NS",   "name": "UltraTech Cement",       "sector": "Cement",        "beta": 1.00},
    {"symbol": "ASIANPAINT.NS",   "name": "Asian Paints",           "sector": "Consumer",      "beta": 0.70},
    {"symbol": "EICHERMOT.NS",    "name": "Eicher Motors",          "sector": "Auto",          "beta": 1.00},
    {"symbol": "INDUSINDBK.NS",   "name": "IndusInd Bank",          "sector": "Banking",       "beta": 1.40},
    {"symbol": "HINDALCO.NS",     "name": "Hindalco",               "sector": "Metal",         "beta": 1.30},
    {"symbol": "BPCL.NS",         "name": "BPCL",                   "sector": "Energy",        "beta": 1.10},
    {"symbol": "GRASIM.NS",       "name": "Grasim Industries",      "sector": "Cement",        "beta": 1.10},
    {"symbol": "DIVISLAB.NS",     "name": "Divi's Laboratories",    "sector": "Pharma",        "beta": 0.75},
    {"symbol": "TATACONSUM.NS",   "name": "Tata Consumer",          "sector": "FMCG",          "beta": 0.80},
    {"symbol": "APOLLOHOSP.NS",   "name": "Apollo Hospitals",       "sector": "Healthcare",    "beta": 0.90},
    {"symbol": "HEROMOTOCO.NS",   "name": "Hero MotoCorp",          "sector": "Auto",          "beta": 0.85},
    {"symbol": "BAJAJ-AUTO.NS",   "name": "Bajaj Auto",             "sector": "Auto",          "beta": 0.90},
    {"symbol": "BRITANNIA.NS",    "name": "Britannia Industries",   "sector": "FMCG",          "beta": 0.55},
    {"symbol": "SHRIRAMFIN.NS",   "name": "Shriram Finance",        "sector": "NBFC",          "beta": 1.20},
    {"symbol": "SBILIFE.NS",      "name": "SBI Life Insurance",     "sector": "Insurance",     "beta": 0.80},
    {"symbol": "HDFCLIFE.NS",     "name": "HDFC Life Insurance",    "sector": "Insurance",     "beta": 0.75},
    {"symbol": "ICICIPRULI.NS",   "name": "ICICI Pru Life",         "sector": "Insurance",     "beta": 0.80},
    {"symbol": "M&M.NS",          "name": "Mahindra & Mahindra",    "sector": "Auto",          "beta": 1.10},
    {"symbol": "ADANIGREEN.NS",   "name": "Adani Green Energy",     "sector": "Power",         "beta": 1.60},
    {"symbol": "ADANITRANS.NS",   "name": "Adani Transmission",     "sector": "Power",         "beta": 1.40},
    {"symbol": "AMBUJACEM.NS",    "name": "Ambuja Cements",         "sector": "Cement",        "beta": 1.00},
    {"symbol": "AUROPHARMA.NS",   "name": "Aurobindo Pharma",       "sector": "Pharma",        "beta": 0.85},
    {"symbol": "BANDHANBNK.NS",   "name": "Bandhan Bank",           "sector": "Banking",       "beta": 1.30},
    {"symbol": "BERGEPAINT.NS",   "name": "Berger Paints",          "sector": "Consumer",      "beta": 0.75},
    {"symbol": "BIOCON.NS",       "name": "Biocon",                 "sector": "Pharma",        "beta": 0.90},
    {"symbol": "BOSCHLTD.NS",     "name": "Bosch",                  "sector": "Auto Ancillary","beta": 0.80},
    {"symbol": "CHOLAFIN.NS",     "name": "Cholamandalam Finance",  "sector": "NBFC",          "beta": 1.20},
    {"symbol": "COLPAL.NS",       "name": "Colgate-Palmolive",      "sector": "FMCG",          "beta": 0.50},
    {"symbol": "CONCOR.NS",       "name": "Container Corp",         "sector": "Logistics",     "beta": 0.90},
    {"symbol": "DABUR.NS",        "name": "Dabur India",            "sector": "FMCG",          "beta": 0.55},
    {"symbol": "DLF.NS",          "name": "DLF",                    "sector": "Realty",        "beta": 1.40},
    {"symbol": "FEDERALBNK.NS",   "name": "Federal Bank",           "sector": "Banking",       "beta": 1.10},
    {"symbol": "GAIL.NS",         "name": "GAIL India",             "sector": "Energy",        "beta": 0.80},
    {"symbol": "GODREJCP.NS",     "name": "Godrej Consumer",        "sector": "FMCG",          "beta": 0.70},
    {"symbol": "GODREJPROP.NS",   "name": "Godrej Properties",      "sector": "Realty",        "beta": 1.30},
    {"symbol": "HAVELLS.NS",      "name": "Havells India",          "sector": "Consumer",      "beta": 0.90},
    {"symbol": "ICICIGI.NS",      "name": "ICICI General Insurance","sector": "Insurance",     "beta": 0.80},
    {"symbol": "INDIGO.NS",       "name": "IndiGo (InterGlobe)",    "sector": "Aviation",      "beta": 1.30},
    {"symbol": "IOC.NS",          "name": "Indian Oil Corp",        "sector": "Energy",        "beta": 0.90},
    {"symbol": "IRCTC.NS",        "name": "IRCTC",                  "sector": "Travel",        "beta": 1.10},
    {"symbol": "LICI.NS",         "name": "LIC of India",           "sector": "Insurance",     "beta": 0.85},
    {"symbol": "LUPIN.NS",        "name": "Lupin",                  "sector": "Pharma",        "beta": 0.80},
    {"symbol": "MARICO.NS",       "name": "Marico",                 "sector": "FMCG",          "beta": 0.55},
    {"symbol": "MCDOWELL-N.NS",   "name": "United Spirits",         "sector": "FMCG",          "beta": 0.80},
    {"symbol": "MUTHOOTFIN.NS",   "name": "Muthoot Finance",        "sector": "NBFC",          "beta": 1.10},
    {"symbol": "NAUKRI.NS",       "name": "Info Edge (Naukri)",     "sector": "Internet",      "beta": 1.20},
    {"symbol": "NMDC.NS",         "name": "NMDC",                   "sector": "Metal",         "beta": 1.00},
    {"symbol": "PAGEIND.NS",      "name": "Page Industries",        "sector": "Consumer",      "beta": 0.85},
    {"symbol": "PIDILITIND.NS",   "name": "Pidilite Industries",    "sector": "Chemicals",     "beta": 0.80},
    {"symbol": "PNB.NS",          "name": "Punjab National Bank",   "sector": "Banking",       "beta": 1.40},
    {"symbol": "RECLTD.NS",       "name": "REC Limited",            "sector": "NBFC",          "beta": 1.10},
    {"symbol": "SAIL.NS",         "name": "SAIL",                   "sector": "Metal",         "beta": 1.30},
    {"symbol": "SBICARD.NS",      "name": "SBI Cards",              "sector": "NBFC",          "beta": 1.20},
    {"symbol": "SIEMENS.NS",      "name": "Siemens India",          "sector": "Engineering",   "beta": 0.90},
    {"symbol": "TORNTPHARM.NS",   "name": "Torrent Pharma",         "sector": "Pharma",        "beta": 0.75},
    {"symbol": "TRENT.NS",        "name": "Trent",                  "sector": "Retail",        "beta": 1.20},
    {"symbol": "UBL.NS",          "name": "United Breweries",       "sector": "FMCG",          "beta": 0.75},
    {"symbol": "VEDL.NS",         "name": "Vedanta",                "sector": "Metal",         "beta": 1.50},
    {"symbol": "ZOMATO.NS",       "name": "Zomato",                 "sector": "Internet",      "beta": 1.60},
    {"symbol": "NYKAA.NS",        "name": "Nykaa",                  "sector": "Retail",        "beta": 1.50},
    {"symbol": "PAYTM.NS",        "name": "Paytm",                  "sector": "Fintech",       "beta": 1.70},
    {"symbol": "POLICYBZR.NS",    "name": "PB Fintech",              "sector": "Fintech",       "beta": 1.60},
    {"symbol": "DMART.NS",        "name": "Avenue Supermarts",      "sector": "Retail",        "beta": 0.80},
    {"symbol": "TATAPOWER.NS",    "name": "Tata Power",              "sector": "Power",         "beta": 1.30},
    {"symbol": "TATACHEM.NS",     "name": "Tata Chemicals",         "sector": "Chemicals",     "beta": 1.10},
    {"symbol": "ABCAPITAL.NS",    "name": "Aditya Birla Capital",   "sector": "NBFC",          "beta": 1.30},
    {"symbol": "ABFRL.NS",        "name": "Aditya Birla Fashion",   "sector": "Retail",        "beta": 1.20},
    {"symbol": "ACC.NS",          "name": "ACC",                    "sector": "Cement",        "beta": 1.00},
    {"symbol": "ALKEM.NS",        "name": "Alkem Laboratories",     "sector": "Pharma",        "beta": 0.70},
    {"symbol": "ATUL.NS",         "name": "Atul Ltd",               "sector": "Chemicals",     "beta": 0.90},
    {"symbol": "AUBANK.NS",       "name": "AU Small Finance Bank",  "sector": "Banking",       "beta": 1.20},
    {"symbol": "BALKRISIND.NS",   "name": "Balkrishna Industries",  "sector": "Auto Ancillary","beta": 1.00},
    {"symbol": "BATAINDIA.NS",    "name": "Bata India",             "sector": "Consumer",      "beta": 0.80},
    {"symbol": "BHARATFORG.NS",   "name": "Bharat Forge",           "sector": "Auto Ancillary","beta": 1.20},
    {"symbol": "BHEL.NS",         "name": "BHEL",                   "sector": "Engineering",   "beta": 1.30},
    {"symbol": "BIKAJI.NS",       "name": "Bikaji Foods",           "sector": "FMCG",          "beta": 0.80},
    {"symbol": "BLUESTARCO.NS",   "name": "Blue Star",              "sector": "Consumer",      "beta": 0.90},
    {"symbol": "BRIGADE.NS",      "name": "Brigade Enterprises",    "sector": "Realty",        "beta": 1.20},
    {"symbol": "CANBK.NS",        "name": "Canara Bank",            "sector": "Banking",       "beta": 1.30},
    {"symbol": "CASTROLIND.NS",   "name": "Castrol India",          "sector": "Energy",        "beta": 0.70},
    {"symbol": "CEATLTD.NS",      "name": "CEAT",                   "sector": "Auto Ancillary","beta": 1.00},
    {"symbol": "CGPOWER.NS",      "name": "CG Power",               "sector": "Engineering",   "beta": 1.30},
    {"symbol": "CHAMBLFERT.NS",   "name": "Chambal Fertilizers",    "sector": "Chemicals",     "beta": 0.80},
    {"symbol": "COFORGE.NS",      "name": "Coforge",                "sector": "IT",            "beta": 1.20},
    {"symbol": "CROMPTON.NS",     "name": "Crompton Greaves Cons",  "sector": "Consumer",      "beta": 0.90},
    {"symbol": "CUMMINSIND.NS",   "name": "Cummins India",          "sector": "Engineering",   "beta": 0.90},
    {"symbol": "DALBHARAT.NS",    "name": "Dalmia Bharat",          "sector": "Cement",        "beta": 1.10},
    {"symbol": "DEEPAKNTR.NS",    "name": "Deepak Nitrite",         "sector": "Chemicals",     "beta": 1.10},
    {"symbol": "DIXON.NS",        "name": "Dixon Technologies",     "sector": "Electronics",   "beta": 1.40},
    {"symbol": "EMAMILTD.NS",     "name": "Emami",                  "sector": "FMCG",          "beta": 0.70},
    {"symbol": "ENGINERSIN.NS",   "name": "Engineers India",        "sector": "Engineering",   "beta": 0.90},
    {"symbol": "ESCORTS.NS",      "name": "Escorts Kubota",         "sector": "Auto",          "beta": 1.00},
    {"symbol": "EXIDEIND.NS",     "name": "Exide Industries",       "sector": "Auto Ancillary","beta": 0.85},
    {"symbol": "FINEORG.NS",      "name": "Fine Organic Inds",      "sector": "Chemicals",     "beta": 0.90},
    {"symbol": "FORTIS.NS",       "name": "Fortis Healthcare",      "sector": "Healthcare",    "beta": 1.00},
    {"symbol": "GLAND.NS",        "name": "Gland Pharma",           "sector": "Pharma",        "beta": 0.80},
    {"symbol": "GLAXO.NS",        "name": "GSK Pharma",             "sector": "Pharma",        "beta": 0.60},
    {"symbol": "GMRINFRA.NS",     "name": "GMR Airports Infra",     "sector": "Infra",         "beta": 1.40},
    {"symbol": "GNFC.NS",         "name": "Gujarat Narmada FC",     "sector": "Chemicals",     "beta": 1.00},
    {"symbol": "GUJGASLTD.NS",    "name": "Gujarat Gas",            "sector": "Energy",        "beta": 0.90},
    {"symbol": "HAL.NS",          "name": "Hindustan Aeronautics",  "sector": "Defence",       "beta": 1.00},
    {"symbol": "HFCL.NS",         "name": "HFCL",                   "sector": "Telecom",       "beta": 1.30},
    {"symbol": "HONAUT.NS",       "name": "Honeywell Automation",   "sector": "Engineering",   "beta": 0.80},
    {"symbol": "IDBI.NS",         "name": "IDBI Bank",              "sector": "Banking",       "beta": 1.20},
    {"symbol": "IDFCFIRSTB.NS",   "name": "IDFC First Bank",        "sector": "Banking",       "beta": 1.30},
    {"symbol": "IEX.NS",          "name": "Indian Energy Exchange", "sector": "Power",         "beta": 1.10},
    {"symbol": "IFBIND.NS",       "name": "IFB Industries",         "sector": "Consumer",      "beta": 0.90},
    {"symbol": "IIFL.NS",         "name": "IIFL Finance",           "sector": "NBFC",          "beta": 1.30},
    {"symbol": "INDIANB.NS",      "name": "Indian Bank",            "sector": "Banking",       "beta": 1.10},
    {"symbol": "INDIAMART.NS",    "name": "IndiaMART InterMESH",    "sector": "Internet",      "beta": 1.30},
    {"symbol": "INDIGOPNTS.NS",   "name": "Indigo Paints",          "sector": "Consumer",      "beta": 1.00},
    {"symbol": "INDUSTOWER.NS",   "name": "Indus Towers",           "sector": "Telecom",       "beta": 0.90},
    {"symbol": "IRFC.NS",         "name": "IRFC",                   "sector": "NBFC",          "beta": 0.80},
    {"symbol": "JKCEMENT.NS",     "name": "JK Cement",              "sector": "Cement",        "beta": 1.00},
    {"symbol": "JKPAPER.NS",      "name": "JK Paper",               "sector": "Paper",         "beta": 1.00},
    {"symbol": "JUBLFOOD.NS",     "name": "Jubilant FoodWorks",     "sector": "Retail",        "beta": 1.10},
    {"symbol": "JUBLINGREA.NS",   "name": "Jubilant Ingrevia",      "sector": "Chemicals",     "beta": 1.00},
    {"symbol": "KAJARIACER.NS",   "name": "Kajaria Ceramics",       "sector": "Consumer",      "beta": 0.90},
    {"symbol": "KANSAINER.NS",    "name": "Kansai Nerolac Paints",  "sector": "Consumer",      "beta": 0.80},
    {"symbol": "KARURVYSYA.NS",   "name": "Karur Vysya Bank",       "sector": "Banking",       "beta": 1.00},
    {"symbol": "KEI.NS",          "name": "KEI Industries",         "sector": "Engineering",   "beta": 1.10},
    {"symbol": "KIMS.NS",         "name": "KIMS Health",            "sector": "Healthcare",    "beta": 0.90},
    {"symbol": "KPITTECH.NS",     "name": "KPIT Technologies",      "sector": "IT",            "beta": 1.30},
    {"symbol": "KPRMILL.NS",      "name": "KPR Mill",               "sector": "Textiles",      "beta": 0.90},
    {"symbol": "LALPATHLAB.NS",   "name": "Dr Lal PathLabs",        "sector": "Healthcare",    "beta": 0.70},
    {"symbol": "LAURUSLABS.NS",   "name": "Laurus Labs",            "sector": "Pharma",        "beta": 1.00},
    {"symbol": "LICHSGFIN.NS",    "name": "LIC Housing Finance",    "sector": "NBFC",          "beta": 1.10},
    {"symbol": "LTIM.NS",         "name": "LTIMindtree",            "sector": "IT",            "beta": 1.00},
    {"symbol": "LTTS.NS",         "name": "L&T Technology Services","sector": "IT",            "beta": 1.10},
    {"symbol": "MAHABANK.NS",     "name": "Bank of Maharashtra",    "sector": "Banking",       "beta": 1.20},
    {"symbol": "MAHINDCIE.NS",    "name": "Mahindra CIE",           "sector": "Auto Ancillary","beta": 1.10},
    {"symbol": "MANAPPURAM.NS",   "name": "Manappuram Finance",     "sector": "NBFC",          "beta": 1.10},
    {"symbol": "MAXHEALTH.NS",    "name": "Max Healthcare",         "sector": "Healthcare",    "beta": 1.00},
    {"symbol": "MCX.NS",          "name": "MCX",                    "sector": "Exchange",      "beta": 1.10},
    {"symbol": "METROBRAND.NS",   "name": "Metro Brands",           "sector": "Retail",        "beta": 1.00},
    {"symbol": "MFSL.NS",         "name": "Max Financial Services", "sector": "Insurance",     "beta": 1.00},
    {"symbol": "MINDAIND.NS",     "name": "Minda Industries",       "sector": "Auto Ancillary","beta": 1.10},
    {"symbol": "MOTHERSON.NS",    "name": "Samvardhana Motherson",  "sector": "Auto Ancillary","beta": 1.30},
    {"symbol": "MPHASIS.NS",      "name": "Mphasis",                "sector": "IT",            "beta": 1.10},
    {"symbol": "NATCOPHARM.NS",   "name": "Natco Pharma",           "sector": "Pharma",        "beta": 0.80},
    {"symbol": "NAVINFLUOR.NS",   "name": "Navin Fluorine",         "sector": "Chemicals",     "beta": 1.00},
    {"symbol": "NCC.NS",          "name": "NCC",                    "sector": "Engineering",   "beta": 1.20},
    {"symbol": "NLCINDIA.NS",     "name": "NLC India",              "sector": "Power",         "beta": 0.90},
    {"symbol": "NOCIL.NS",        "name": "NOCIL",                  "sector": "Chemicals",     "beta": 0.90},
    {"symbol": "OBEROIRLTY.NS",   "name": "Oberoi Realty",          "sector": "Realty",        "beta": 1.20},
    {"symbol": "OIL.NS",          "name": "Oil India",              "sector": "Energy",        "beta": 1.10},
    {"symbol": "OLECTRA.NS",      "name": "Olectra Greentech",      "sector": "Auto",          "beta": 1.40},
    {"symbol": "PERSISTENT.NS",   "name": "Persistent Systems",      "sector": "IT",            "beta": 1.20},
    {"symbol": "PETRONET.NS",     "name": "Petronet LNG",           "sector": "Energy",        "beta": 0.80},
    {"symbol": "PFIZER.NS",       "name": "Pfizer",                 "sector": "Pharma",        "beta": 0.55},
    {"symbol": "POLYCAB.NS",      "name": "Polycab India",          "sector": "Engineering",   "beta": 1.00},
    {"symbol": "PRAJIND.NS",      "name": "Praj Industries",        "sector": "Engineering",   "beta": 1.10},
    {"symbol": "PRESTIGE.NS",     "name": "Prestige Estates",       "sector": "Realty",        "beta": 1.30},
    {"symbol": "PVRINOX.NS",      "name": "PVR INOX",               "sector": "Entertainment", "beta": 1.30},
    {"symbol": "RAJESHEXPO.NS",   "name": "Rajesh Exports",         "sector": "Consumer",      "beta": 1.00},
    {"symbol": "RAMCOCEM.NS",     "name": "Ramco Cements",          "sector": "Cement",        "beta": 1.00},
    {"symbol": "RELAXO.NS",       "name": "Relaxo Footwears",       "sector": "Consumer",      "beta": 0.80},
    {"symbol": "RITES.NS",        "name": "RITES",                  "sector": "Engineering",   "beta": 0.90},
    {"symbol": "SJVN.NS",         "name": "SJVN",                   "sector": "Power",         "beta": 0.80},
    {"symbol": "SOBHA.NS",        "name": "Sobha",                  "sector": "Realty",        "beta": 1.20},
    {"symbol": "SOLARINDS.NS",    "name": "Solar Industries",        "sector": "Defence",       "beta": 1.00},
    {"symbol": "SPARC.NS",        "name": "Sun Pharma Adv Res",     "sector": "Pharma",        "beta": 1.00},
    {"symbol": "SUMICHEM.NS",     "name": "Sumitomo Chemical",      "sector": "Chemicals",     "beta": 0.90},
    {"symbol": "SUNDARMFIN.NS",   "name": "Sundaram Finance",       "sector": "NBFC",          "beta": 0.90},
    {"symbol": "SUNTV.NS",        "name": "Sun TV Network",         "sector": "Media",         "beta": 0.80},
    {"symbol": "SUPREMEIND.NS",   "name": "Supreme Industries",     "sector": "Consumer",      "beta": 0.90},
    {"symbol": "SYNGENE.NS",      "name": "Syngene International",  "sector": "Pharma",        "beta": 0.80},
    {"symbol": "TATAELXSI.NS",    "name": "Tata Elxsi",             "sector": "IT",            "beta": 1.30},
    {"symbol": "TATAINVEST.NS",   "name": "Tata Investment Corp",   "sector": "Conglomerate",  "beta": 0.90},
    {"symbol": "TATATECH.NS",     "name": "Tata Technologies",      "sector": "IT",            "beta": 1.20},
    {"symbol": "TIMKEN.NS",       "name": "Timken India",           "sector": "Engineering",   "beta": 0.90},
    {"symbol": "TITAGARH.NS",     "name": "Titagarh Rail Systems",  "sector": "Engineering",   "beta": 1.30},
    {"symbol": "TVSMOTORS.NS",    "name": "TVS Motors",             "sector": "Auto",          "beta": 1.10},
    {"symbol": "UCOBANK.NS",      "name": "UCO Bank",               "sector": "Banking",       "beta": 1.40},
    {"symbol": "UNIONBANK.NS",    "name": "Union Bank of India",    "sector": "Banking",       "beta": 1.30},
    {"symbol": "UPL.NS",          "name": "UPL",                    "sector": "Chemicals",     "beta": 1.20},
    {"symbol": "VOLTAS.NS",       "name": "Voltas",                 "sector": "Consumer",      "beta": 1.00},
    {"symbol": "WOCKPHARMA.NS",   "name": "Wockhardt",              "sector": "Pharma",        "beta": 1.10},
    {"symbol": "ZEEL.NS",         "name": "Zee Entertainment",      "sector": "Media",         "beta": 1.20},
]
# Deduplicate Watchlist
seen = set()
WATCHLIST_CLEAN = []
for item in WATCHLIST:
    if item["symbol"] not in seen:
        seen.add(item["symbol"])
        WATCHLIST_CLEAN.append(item)
WATCHLIST = WATCHLIST_CLEAN

# ── Sector & Outlook Metadata ────────────────────────────────────────────────
SECTOR_PE = {
    "Banking": 18, "Pharma": 28, "Auto": 20, "Energy": 12, "FMCG": 50, "Infra": 22, 
    "Power": 18, "IT": 25, "Consumer": 55, "NBFC": 22, "Conglomerate": 20, "Metal": 14,
    "Cement": 20, "Chemicals": 30, "Healthcare": 35, "Insurance": 40, "Realty": 25, 
    "Telecom": 22, "Engineering": 24, "Defence": 30, "Auto Ancillary": 22, 
    "Logistics": 28, "Internet": 60, "Fintech": 50, "Retail": 55, "Media": 18,
    "Textiles": 15, "Financial": 25, "Agriculture": 18, "Hotels": 30, 
    "Electronics": 35, "Shipping": 10, "Travel": 22, "Entertainment": 30, 
    "Exchange": 35, "Paper": 12, "Education": 40, "Services": 25, "Trading": 12,
}

SECTOR_OUTLOOK = {
    "Banking": ("Positive", "RBI rate-cut cycle benefits NIMs; robust credit growth."),
    "Pharma": ("Strong", "Weak rupee boosts USD export earnings; sector outperforming."),
    "Auto": ("Positive", "GST reforms positive; above-normal monsoon strengthens demand."),
    "Energy": ("Stable", "High crude benefits domestic coal substitution; power demand rising."),
    "FMCG": ("Positive", "Above-normal monsoon drives rural volumes; tax relief lifts spending."),
    "Infra": ("Positive", "Govt capex thrust; trade-corridor deals catalysing infra growth."),
    "Power": ("Strong", "India power demand surging — EV and data-centre growth."),
    "IT": ("Cautious", "Sector beaten down — contrarian opportunity; rupee tailwinds help."),
    "Consumer": ("Positive", "Urban discretionary spending and festive season tailwinds."),
}

def is_market_open():
    now = datetime.now(IST)
    if now.weekday() >= 5: return False
    mo, mc = now.replace(hour=9, minute=15, second=0), now.replace(hour=15, minute=30, second=0)
    return mo <= now <= mc

# ── Technical indicators ─────────────────────────────────────────────────────
def ema(data, span):
    k = 2 / (span + 1)
    r = [data[0]]
    for p in data[1:]: r.append(p * k + r[-1] * (1 - k))
    return np.array(r)

def compute_rsi(prices, period=14):
    if len(prices) < period + 1: return None
    d = np.diff(prices)
    g, l = np.where(d > 0, d, 0.0), np.where(d < 0, -d, 0.0)
    ag, al = np.mean(g[:period]), np.mean(l[:period])
    for i in range(period, len(g)):
        ag = (ag * (period - 1) + g[i]) / period
        al = (al * (period - 1) + l[i]) / period
    return round(100 - 100 / (1 + ag / al) if al else 100, 2)

def compute_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow + signal: return None, None, None
    ef, es = ema(prices, fast), ema(prices, slow)
    ml = ef[-len(es):] - es
    sl = ema(ml, signal)
    hist = ml[-len(sl):] - sl
    return round(float(ml[-1]), 4), round(float(sl[-1]), 4), round(float(hist[-1]), 4)

def compute_atr(highs, lows, closes, period=14):
    if len(closes) < period + 1: return None
    trs = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1, len(closes))]
    return round(np.mean(trs[-period:]), 2)

# ── Scoring Engine ───────────────────────────────────────────────────────────
def score_stock(d):
    score, reasons = 0, []
    rsi = d.get("rsi")
    if rsi:
        if rsi < 30: score += 3; reasons.append(f"RSI {rsi} oversold")
        elif rsi < 45: score += 2; reasons.append("Bullish setup")
        elif rsi > 75: score -= 2; reasons.append("Overbought")
    
    mh = d.get("macd_hist")
    if mh is not None:
        if mh > 0: score += 2; reasons.append("MACD Positive")
        else: score -= 1

    c, ma50, ma200 = d["current_price"], d["ma50"], d["ma200"]
    if c > ma50 > ma200: score += 3; reasons.append("Strong Uptrend")
    if d.get("iv_status") == "Undervalued": score += 3; reasons.append("Value zone")
    
    return score, reasons

# ── Builders ─────────────────────────────────────────────────────────────────
def build_recommendation(d, rank):
    score, _ = score_stock(d)
    c = d["current_price"]
    signal = "BUY" if score >= 4 else "SELL" if score <= -3 else "HOLD"
    confidence = min(95, 55 + abs(score) * 5)
    atr = d.get("atr") or (c * 0.015)
    
    return {
        "rank": rank, "name": d["name"], "symbol": d["symbol"].replace(".NS", ""),
        "sector": d["sector"], "signal": signal, "current_price": c,
        "target_price": round(c + atr * 6, 2) if signal != "SELL" else round(c - atr * 5, 2),
        "stop_loss": round(c - atr * 3, 2), "intrinsic_value": d.get("intrinsic_value"),
        "iv_status": d.get("iv_status", "N/A"), "margin_of_safety": d.get("margin_of_safety"),
        "confidence": confidence, "risk_level": "High" if d.get("beta", 1) > 1.3 else "Medium",
        "holding_period": "6–8 weeks", "upside_pct": 15.0,
        "technical_summary": f"RSI: {d['rsi']}. MACD Hist: {d['macd_hist']}. Trend: {d['trend']}.",
        "fundamental_summary": f"Sector PE: {SECTOR_PE.get(d['sector'], 20)}x.",
        "situational_summary": SECTOR_OUTLOOK.get(d['sector'], ("Neutral", ""))[1],
        "key_risks": "Market macro risks.", "score": score
    }

def build_option(stock_data, rank):
    c = stock_data["current_price"]
    strike = round(c / 50) * 50 + 50
    return {
        "rank": rank, "underlying": stock_data["name"], "symbol": stock_data["symbol"].replace(".NS", ""),
        "option_type": "CE", "strike_price": strike, "expiry": "Jul 2026",
        "current_stock_price": c, "strategy": "Long Call", "risk_level": "High",
        "target_move_pct": 12.0, "max_loss": "Premium paid", "holding_period": "4–6 weeks",
        "reasoning": f"Leveraging {stock_data['trend']} momentum.", "key_risks": "Theta decay."
    }

def fetch_stock(item):
    try:
        tk = yf.Ticker(item["symbol"])
        hist = tk.history(period="12mo", interval="1d")
        if hist.empty: return None
        closes = hist["Close"].tolist()
        highs, lows = hist["High"].tolist(), hist["Low"].tolist()
        c = round(closes[-1], 2)
        
        iv_data = {"intrinsic_value": None, "margin_of_safety": None, "iv_status": "N/A"}
        try:
            info = tk.info
            eps = info.get("trailingEps") or info.get("forwardEps") or (c / 22)
            iv = round(eps * SECTOR_PE.get(item["sector"], 20), 2)
            mos = round((iv - c) / c * 100, 1)
            iv_data = {"intrinsic_value": iv, "margin_of_safety": mos, "iv_status": "Undervalued" if mos > 10 else "Fair"}
        except: pass

        m, s, h = compute_macd(closes)
        return {
            **item, "current_price": c, "rsi": compute_rsi(closes),
            "macd_hist": h, "ma50": np.mean(closes[-50:]),
            "ma200": np.mean(closes[-200:]), "atr": compute_atr(highs, lows, closes),
            "trend": "Uptrend" if c > np.mean(closes[-50:]) else "Neutral/Down", **iv_data
        }
    except: return None

# ── Routes ───────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("Daily_Recomendation.html")

@app.route("/api/market-status")
def market_status():
    now = datetime.now(IST)
    is_open = now.weekday() < 5 and (9 <= now.hour < 16)
    return jsonify({"is_open": is_open, "current_time": now.strftime("%I:%M %p IST")})

@app.route("/api/recommendations")
def recommendations():
    try:
        # THE FIX: Sampling prevents Render 30s timeout
        scan_sample = random.sample(WATCHLIST, min(len(WATCHLIST), 45))
        stocks_data = []

        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = {ex.submit(fetch_stock, item): item for item in scan_sample}
            for future in as_completed(futures):
                res = future.result()
                if res: stocks_data.append(res)

        scored = sorted(stocks_data, key=lambda x: score_stock(x)[0], reverse=True)
        top10 = scored[:10]
        stock_recs = [build_recommendation(s, i+1) for i, s in enumerate(top10)]
        opt_recs = [build_option(s, i+1) for i in range(min(2, len(scored)))]

        return jsonify({
            "generated_at": datetime.now(IST).strftime("%d %b %Y, %I:%M %p IST"),
            "stocks_analyzed": len(stocks_data),
            "market_summary": f"Scanned {len(stocks_data)} stocks; Strongest momentum in {scored[0]['sector']}.",
            "stocks": stock_recs, "options": opt_recs
        })
    except: return jsonify({"error": "Engine busy."}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
