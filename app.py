import os
import json
import time
import logging
import calendar
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

# ── 2000+ NSE stocks ─────────────────────────────────────────────────────────
# Format: {"symbol": "XYZ.NS", "name": "Company Name", "sector": "Sector", "beta": 1.0}
# Covers: Nifty 50, Nifty Next 50, Nifty Midcap 150, Nifty Smallcap 250,
#         plus additional liquid NSE-listed stocks across all sectors

WATCHLIST = [
    # ── NIFTY 50 ──────────────────────────────────────────────────────────────
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
    {"symbol": "POWERGRID.NS",    "name": "Power Grid",             "sector": "Power",         "beta": 0.60},
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

    # ── NIFTY NEXT 50 ─────────────────────────────────────────────────────────
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
    {"symbol": "NYKAA.NS",        "name": "Nykaa (FSN E-Commerce)", "sector": "Retail",        "beta": 1.50},
    {"symbol": "PAYTM.NS",        "name": "Paytm (One97 Comm)",     "sector": "Fintech",       "beta": 1.70},
    {"symbol": "POLICYBZR.NS",    "name": "PB Fintech",             "sector": "Fintech",       "beta": 1.60},
    {"symbol": "DMART.NS",        "name": "Avenue Supermarts",      "sector": "Retail",        "beta": 0.80},
    {"symbol": "TATAPOWER.NS",    "name": "Tata Power",             "sector": "Power",         "beta": 1.30},
    {"symbol": "TATACHEM.NS",     "name": "Tata Chemicals",         "sector": "Chemicals",     "beta": 1.10},
    {"symbol": "VEDL.NS",         "name": "Vedanta",                "sector": "Metal",         "beta": 1.50},
    {"symbol": "WIPRO.NS",        "name": "Wipro",                  "sector": "IT",            "beta": 0.80},

    # ── NIFTY MIDCAP 150 ──────────────────────────────────────────────────────
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
    {"symbol": "MARICO.NS",       "name": "Marico",                 "sector": "FMCG",          "beta": 0.55},
    {"symbol": "MAX.NS",          "name": "Max Financial Services", "sector": "Insurance",     "beta": 1.10},
    {"symbol": "MAXHEALTH.NS",    "name": "Max Healthcare",         "sector": "Healthcare",    "beta": 1.00},
    {"symbol": "MCX.NS",          "name": "MCX",                    "sector": "Exchange",      "beta": 1.10},
    {"symbol": "METROBRAND.NS",   "name": "Metro Brands",           "sector": "Retail",        "beta": 1.00},
    {"symbol": "MFSL.NS",         "name": "Max Financial Services", "sector": "Insurance",     "beta": 1.00},
    {"symbol": "MINDAIND.NS",     "name": "Minda Industries",       "sector": "Auto Ancillary","beta": 1.10},
    {"symbol": "MMTC.NS",         "name": "MMTC",                   "sector": "Trading",       "beta": 1.10},
    {"symbol": "MOTHERSON.NS",    "name": "Samvardhana Motherson",  "sector": "Auto Ancillary","beta": 1.30},
    {"symbol": "MPHASIS.NS",      "name": "Mphasis",                "sector": "IT",            "beta": 1.10},
    {"symbol": "MRF.NS",          "name": "MRF",                    "sector": "Auto Ancillary","beta": 0.80},
    {"symbol": "NATCOPHARM.NS",   "name": "Natco Pharma",           "sector": "Pharma",        "beta": 0.80},
    {"symbol": "NAVINFLUOR.NS",   "name": "Navin Fluorine",         "sector": "Chemicals",     "beta": 1.00},
    {"symbol": "NCC.NS",          "name": "NCC",                    "sector": "Engineering",   "beta": 1.20},
    {"symbol": "NLCINDIA.NS",     "name": "NLC India",              "sector": "Power",         "beta": 0.90},
    {"symbol": "NOCIL.NS",        "name": "NOCIL",                  "sector": "Chemicals",     "beta": 0.90},
    {"symbol": "OBEROIRLTY.NS",   "name": "Oberoi Realty",          "sector": "Realty",        "beta": 1.20},
    {"symbol": "OIL.NS",          "name": "Oil India",              "sector": "Energy",        "beta": 1.10},
    {"symbol": "OLECTRA.NS",      "name": "Olectra Greentech",      "sector": "Auto",          "beta": 1.40},
    {"symbol": "PGHH.NS",         "name": "P&G Hygiene",            "sector": "FMCG",          "beta": 0.50},
    {"symbol": "PERSISTENT.NS",   "name": "Persistent Systems",     "sector": "IT",            "beta": 1.20},
    {"symbol": "PETRONET.NS",     "name": "Petronet LNG",           "sector": "Energy",        "beta": 0.80},
    {"symbol": "PFIZER.NS",       "name": "Pfizer",                 "sector": "Pharma",        "beta": 0.55},
    {"symbol": "PHOENIXLTD.NS",   "name": "Phoenix Mills",          "sector": "Realty",        "beta": 1.20},
    {"symbol": "POLYCAB.NS",      "name": "Polycab India",          "sector": "Engineering",   "beta": 1.00},
    {"symbol": "PRAJIND.NS",      "name": "Praj Industries",        "sector": "Engineering",   "beta": 1.10},
    {"symbol": "PRESTIGE.NS",     "name": "Prestige Estates",       "sector": "Realty",        "beta": 1.30},
    {"symbol": "PRINCEPIPE.NS",   "name": "Prince Pipes",           "sector": "Consumer",      "beta": 1.00},
    {"symbol": "PVRINOX.NS",      "name": "PVR INOX",               "sector": "Entertainment", "beta": 1.30},
    {"symbol": "RAJESHEXPO.NS",   "name": "Rajesh Exports",         "sector": "Consumer",      "beta": 1.00},
    {"symbol": "RAMCOCEM.NS",     "name": "Ramco Cements",          "sector": "Cement",        "beta": 1.00},
    {"symbol": "RELAXO.NS",       "name": "Relaxo Footwears",       "sector": "Consumer",      "beta": 0.80},
    {"symbol": "RITES.NS",        "name": "RITES",                  "sector": "Engineering",   "beta": 0.90},
    {"symbol": "SJVN.NS",         "name": "SJVN",                   "sector": "Power",         "beta": 0.80},
    {"symbol": "SOBHA.NS",        "name": "Sobha",                  "sector": "Realty",        "beta": 1.20},
    {"symbol": "SOLARINDS.NS",    "name": "Solar Industries",       "sector": "Defence",       "beta": 1.00},
    {"symbol": "SPARC.NS",        "name": "Sun Pharma Adv Res",     "sector": "Pharma",        "beta": 1.00},
    {"symbol": "STAR.NS",         "name": "Star Health Insurance",  "sector": "Insurance",     "beta": 1.00},
    {"symbol": "STARHEALTH.NS",   "name": "Star Health",            "sector": "Insurance",     "beta": 1.00},
    {"symbol": "SUMICHEM.NS",     "name": "Sumitomo Chemical",      "sector": "Chemicals",     "beta": 0.90},
    {"symbol": "SUNDARMFIN.NS",   "name": "Sundaram Finance",       "sector": "NBFC",          "beta": 0.90},
    {"symbol": "SUNDRMFAST.NS",   "name": "Sundram Fasteners",      "sector": "Auto Ancillary","beta": 0.90},
    {"symbol": "SUNTV.NS",        "name": "Sun TV Network",         "sector": "Media",         "beta": 0.80},
    {"symbol": "SUPREMEIND.NS",   "name": "Supreme Industries",     "sector": "Consumer",      "beta": 0.90},
    {"symbol": "SYNGENE.NS",      "name": "Syngene International",  "sector": "Pharma",        "beta": 0.80},
    {"symbol": "TANLA.NS",        "name": "Tanla Platforms",        "sector": "IT",            "beta": 1.30},
    {"symbol": "TATACOFFEE.NS",   "name": "Tata Coffee",            "sector": "FMCG",          "beta": 0.80},
    {"symbol": "TATAELXSI.NS",    "name": "Tata Elxsi",             "sector": "IT",            "beta": 1.30},
    {"symbol": "TATAINVEST.NS",   "name": "Tata Investment Corp",   "sector": "Conglomerate",  "beta": 0.90},
    {"symbol": "TATATECH.NS",     "name": "Tata Technologies",      "sector": "IT",            "beta": 1.20},
    {"symbol": "TEAMLEASE.NS",    "name": "TeamLease Services",     "sector": "Services",      "beta": 1.10},
    {"symbol": "THYROCARE.NS",    "name": "Thyrocare Technologies", "sector": "Healthcare",    "beta": 0.80},
    {"symbol": "TIMKEN.NS",       "name": "Timken India",           "sector": "Engineering",   "beta": 0.90},
    {"symbol": "TITAGARH.NS",     "name": "Titagarh Rail Systems",  "sector": "Engineering",   "beta": 1.30},
    {"symbol": "TORNTPOWER.NS",   "name": "Torrent Power",          "sector": "Power",         "beta": 0.80},
    {"symbol": "TTKPRESTIG.NS",   "name": "TTK Prestige",           "sector": "Consumer",      "beta": 0.80},
    {"symbol": "TVSMOTORS.NS",    "name": "TVS Motors",             "sector": "Auto",          "beta": 1.10},
    {"symbol": "UBLLTD.NS",       "name": "United Breweries",       "sector": "FMCG",          "beta": 0.75},
    {"symbol": "UCOBANK.NS",      "name": "UCO Bank",               "sector": "Banking",       "beta": 1.40},
    {"symbol": "UNIONBANK.NS",    "name": "Union Bank of India",    "sector": "Banking",       "beta": 1.30},
    {"symbol": "UNITDSPR.NS",     "name": "United Spirits",         "sector": "FMCG",          "beta": 0.80},
    {"symbol": "UPL.NS",          "name": "UPL",                    "sector": "Chemicals",     "beta": 1.20},
    {"symbol": "UTIAMC.NS",       "name": "UTI Asset Management",   "sector": "Financial",     "beta": 1.00},
    {"symbol": "VAIBHAVGBL.NS",   "name": "Vaibhav Global",         "sector": "Retail",        "beta": 1.00},
    {"symbol": "VGUARD.NS",       "name": "V-Guard Industries",     "sector": "Consumer",      "beta": 0.90},
    {"symbol": "VINATIORGA.NS",   "name": "Vinati Organics",        "sector": "Chemicals",     "beta": 0.90},
    {"symbol": "VOLTAS.NS",       "name": "Voltas",                 "sector": "Consumer",      "beta": 1.00},
    {"symbol": "WHIRLPOOL.NS",    "name": "Whirlpool of India",     "sector": "Consumer",      "beta": 0.85},
    {"symbol": "WIPRO.NS",        "name": "Wipro",                  "sector": "IT",            "beta": 0.80},
    {"symbol": "WOCKPHARMA.NS",   "name": "Wockhardt",              "sector": "Pharma",        "beta": 1.10},
    {"symbol": "ZEEL.NS",         "name": "Zee Entertainment",      "sector": "Media",         "beta": 1.20},
    {"symbol": "ZENSARTECH.NS",   "name": "Zensar Technologies",    "sector": "IT",            "beta": 1.10},
    {"symbol": "ZENTEC.NS",       "name": "Zen Technologies",       "sector": "Defence",       "beta": 1.30},

    # ── NIFTY SMALLCAP / ADDITIONAL LIQUID NSE STOCKS ────────────────────────
    {"symbol": "AARTIIND.NS",     "name": "Aarti Industries",       "sector": "Chemicals",     "beta": 1.10},
    {"symbol": "AARTIPHARM.NS",   "name": "Aarti Pharmalabs",       "sector": "Pharma",        "beta": 1.00},
    {"symbol": "ABBOTINDIA.NS",   "name": "Abbott India",           "sector": "Pharma",        "beta": 0.60},
    {"symbol": "ABFRL.NS",        "name": "ABFRL",                  "sector": "Retail",        "beta": 1.20},
    {"symbol": "ACE.NS",          "name": "Action Construction Eq", "sector": "Engineering",   "beta": 1.20},
    {"symbol": "ADANIPOWER.NS",   "name": "Adani Power",            "sector": "Power",         "beta": 1.50},
    {"symbol": "AEGISCHEM.NS",    "name": "Aegis Chemicals",        "sector": "Chemicals",     "beta": 1.00},
    {"symbol": "AFFLE.NS",        "name": "Affle India",            "sector": "Internet",      "beta": 1.40},
    {"symbol": "AJANTPHARM.NS",   "name": "Ajanta Pharma",          "sector": "Pharma",        "beta": 0.80},
    {"symbol": "AKZOINDIA.NS",    "name": "Akzo Nobel India",       "sector": "Chemicals",     "beta": 0.70},
    {"symbol": "ALEMBICLTD.NS",   "name": "Alembic",                "sector": "Pharma",        "beta": 0.80},
    {"symbol": "ALEXLAB.NS",      "name": "Alexlab",                "sector": "Healthcare",    "beta": 0.90},
    {"symbol": "ALKYLAMINE.NS",   "name": "Alkyl Amines Chem",      "sector": "Chemicals",     "beta": 1.00},
    {"symbol": "ALLCARGO.NS",     "name": "Allcargo Logistics",     "sector": "Logistics",     "beta": 1.10},
    {"symbol": "ALOKINDS.NS",     "name": "Alok Industries",        "sector": "Textiles",      "beta": 1.50},
    {"symbol": "AMARAJABAT.NS",   "name": "Amara Raja Energy",      "sector": "Auto Ancillary","beta": 0.90},
    {"symbol": "AMBER.NS",        "name": "Amber Enterprises",      "sector": "Electronics",   "beta": 1.20},
    {"symbol": "AMJUMBOCOM.NS",   "name": "Ambo Agri",              "sector": "Agriculture",   "beta": 0.90},
    {"symbol": "ANANTRAJ.NS",     "name": "Anant Raj",              "sector": "Realty",        "beta": 1.30},
    {"symbol": "ANGELONE.NS",     "name": "Angel One",              "sector": "Financial",     "beta": 1.40},
    {"symbol": "APARINDS.NS",     "name": "Apar Industries",        "sector": "Engineering",   "beta": 1.10},
    {"symbol": "APTUS.NS",        "name": "Aptus Value Housing",    "sector": "NBFC",          "beta": 1.00},
    {"symbol": "ARCHIES.NS",      "name": "Archies",                "sector": "Retail",        "beta": 0.90},
    {"symbol": "ARVINDFASN.NS",   "name": "Arvind Fashions",        "sector": "Retail",        "beta": 1.20},
    {"symbol": "ARVINDLTD.NS",    "name": "Arvind Ltd",             "sector": "Textiles",      "beta": 1.10},
    {"symbol": "ASAHIINDIA.NS",   "name": "Asahi India Glass",      "sector": "Auto Ancillary","beta": 0.90},
    {"symbol": "ASHIANA.NS",      "name": "Ashiana Housing",        "sector": "Realty",        "beta": 1.00},
    {"symbol": "ASHOKLEY.NS",     "name": "Ashok Leyland",          "sector": "Auto",          "beta": 1.20},
    {"symbol": "ASTERDM.NS",      "name": "Aster DM Healthcare",    "sector": "Healthcare",    "beta": 1.00},
    {"symbol": "ASTRAL.NS",       "name": "Astral",                 "sector": "Consumer",      "beta": 1.00},
    {"symbol": "ASTRAZEN.NS",     "name": "AstraZeneca Pharma",     "sector": "Pharma",        "beta": 0.70},
    {"symbol": "ATGL.NS",         "name": "Adani Total Gas",        "sector": "Energy",        "beta": 1.50},
    {"symbol": "ATUL.NS",         "name": "Atul Ltd",               "sector": "Chemicals",     "beta": 0.90},
    {"symbol": "AVANTIFEED.NS",   "name": "Avanti Feeds",           "sector": "Agriculture",   "beta": 1.00},
    {"symbol": "BAJAJHLDNG.NS",   "name": "Bajaj Holdings",         "sector": "Conglomerate",  "beta": 0.90},
    {"symbol": "BALMLAWRIE.NS",   "name": "Balmer Lawrie",          "sector": "Logistics",     "beta": 0.80},
    {"symbol": "BANSALFIN.NS",    "name": "Bansal Finance",         "sector": "NBFC",          "beta": 1.10},
    {"symbol": "BALAMINES.NS",    "name": "Balaji Amines",          "sector": "Chemicals",     "beta": 1.10},
    {"symbol": "BASF.NS",         "name": "BASF India",             "sector": "Chemicals",     "beta": 0.90},
    {"symbol": "BAYERCROP.NS",    "name": "Bayer CropScience",      "sector": "Agriculture",   "beta": 0.70},
    {"symbol": "BBTC.NS",         "name": "Bombay Burmah Trading",  "sector": "Conglomerate",  "beta": 0.90},
    {"symbol": "BEML.NS",         "name": "BEML",                   "sector": "Engineering",   "beta": 1.20},
    {"symbol": "BENGALASM.NS",    "name": "Bengal & Assam Co",      "sector": "Conglomerate",  "beta": 0.80},
    {"symbol": "BIGBLOC.NS",      "name": "Bigbloc Construction",   "sector": "Realty",        "beta": 1.20},
    {"symbol": "BLKASHYAP.NS",    "name": "B L Kashyap",            "sector": "Engineering",   "beta": 1.10},
    {"symbol": "BLUEDART.NS",     "name": "Blue Dart Express",      "sector": "Logistics",     "beta": 0.70},
    {"symbol": "BORORENEW.NS",    "name": "Borosil Renewables",     "sector": "Power",         "beta": 1.20},
    {"symbol": "BEL.NS",          "name": "Bharat Electronics",     "sector": "Defence",       "beta": 1.00},
    {"symbol": "BFINVEST.NS",     "name": "BF Investment",          "sector": "Conglomerate",  "beta": 1.00},
    {"symbol": "CALSOFT.NS",      "name": "California Software",    "sector": "IT",            "beta": 1.00},
    {"symbol": "CAPLIPOINT.NS",   "name": "Caplin Point Labs",      "sector": "Pharma",        "beta": 0.90},
    {"symbol": "CARBORUNIV.NS",   "name": "Carborundum Universal",  "sector": "Engineering",   "beta": 1.00},
    {"symbol": "CCL.NS",          "name": "CCL Products",           "sector": "FMCG",          "beta": 0.80},
    {"symbol": "CDSL.NS",         "name": "CDSL",                   "sector": "Financial",     "beta": 1.20},
    {"symbol": "CENTURYPLY.NS",   "name": "Century Plyboards",      "sector": "Consumer",      "beta": 1.00},
    {"symbol": "CENTURYTEX.NS",   "name": "Century Textiles",       "sector": "Textiles",      "beta": 1.00},
    {"symbol": "CESC.NS",         "name": "CESC",                   "sector": "Power",         "beta": 0.80},
    {"symbol": "CHEMPLASTS.NS",   "name": "Chemplast Sanmar",       "sector": "Chemicals",     "beta": 1.10},
    {"symbol": "CIGNITITEC.NS",   "name": "Cigniti Technologies",   "sector": "IT",            "beta": 1.20},
    {"symbol": "CLEAN.NS",        "name": "Clean Science & Tech",   "sector": "Chemicals",     "beta": 1.00},
    {"symbol": "COCHINSHIP.NS",   "name": "Cochin Shipyard",        "sector": "Defence",       "beta": 1.10},
    {"symbol": "CRAFTSMAN.NS",    "name": "Craftsman Automation",   "sector": "Auto Ancillary","beta": 1.10},
    {"symbol": "CREMACROP.NS",    "name": "Crema Crop",             "sector": "Agriculture",   "beta": 1.00},
    {"symbol": "CYIENT.NS",       "name": "Cyient",                 "sector": "IT",            "beta": 1.10},
    {"symbol": "DATAMATICS.NS",   "name": "Datamatics Global",      "sector": "IT",            "beta": 1.00},
    {"symbol": "DCMSHRIRAM.NS",   "name": "DCM Shriram",            "sector": "Chemicals",     "beta": 0.90},
    {"symbol": "DELHIVERY.NS",    "name": "Delhivery",              "sector": "Logistics",     "beta": 1.30},
    {"symbol": "DEVYANI.NS",      "name": "Devyani International",  "sector": "Retail",        "beta": 1.20},
    {"symbol": "DFMFOODS.NS",     "name": "DFM Foods",              "sector": "FMCG",          "beta": 0.70},
    {"symbol": "DHANI.NS",        "name": "Dhani Services",         "sector": "Fintech",       "beta": 1.50},
    {"symbol": "DHARMAJ.NS",      "name": "Dharmaj Crop Guard",     "sector": "Agriculture",   "beta": 1.00},
    {"symbol": "DHANUKA.NS",      "name": "Dhanuka Agritech",       "sector": "Agriculture",   "beta": 0.80},
    {"symbol": "DODLA.NS",        "name": "Dodla Dairy",            "sector": "FMCG",          "beta": 0.80},
    {"symbol": "DPWWORLD.NS",     "name": "DP World",               "sector": "Logistics",     "beta": 1.00},
    {"symbol": "EDELWEISS.NS",    "name": "Edelweiss Financial",    "sector": "Financial",     "beta": 1.30},
    {"symbol": "ELGIEQUIP.NS",    "name": "Elgi Equipments",        "sector": "Engineering",   "beta": 0.90},
    {"symbol": "EMKAY.NS",        "name": "Emkay Global",           "sector": "Financial",     "beta": 1.20},
    {"symbol": "EPIGRAL.NS",      "name": "Epigral",                "sector": "Chemicals",     "beta": 1.10},
    {"symbol": "EQUITASBNK.NS",   "name": "Equitas Small Fin Bank", "sector": "Banking",       "beta": 1.20},
    {"symbol": "ESABINDIA.NS",    "name": "Esab India",             "sector": "Engineering",   "beta": 0.80},
    {"symbol": "ESCONINDS.NS",    "name": "Escon Inds",             "sector": "Engineering",   "beta": 1.00},
    {"symbol": "ETHOSLTD.NS",     "name": "Ethos",                  "sector": "Retail",        "beta": 1.10},
    {"symbol": "EWAY.NS",         "name": "Eveready Industries",    "sector": "Consumer",      "beta": 0.90},
    {"symbol": "FINCABLES.NS",    "name": "Finolex Cables",         "sector": "Engineering",   "beta": 0.90},
    {"symbol": "FINOLEXIND.NS",   "name": "Finolex Industries",     "sector": "Consumer",      "beta": 0.90},
    {"symbol": "FLAIR.NS",        "name": "Flair Writing",          "sector": "Consumer",      "beta": 0.90},
    {"symbol": "FLUOROCHEM.NS",   "name": "Gujarat Fluorochem",     "sector": "Chemicals",     "beta": 1.10},
    {"symbol": "GABRIEL.NS",      "name": "Gabriel India",          "sector": "Auto Ancillary","beta": 0.90},
    {"symbol": "GALAXYSURF.NS",   "name": "Galaxy Surfactants",     "sector": "Chemicals",     "beta": 0.90},
    {"symbol": "GARFIBRES.NS",    "name": "Garware Technical Fib",  "sector": "Textiles",      "beta": 0.90},
    {"symbol": "GESHIP.NS",       "name": "Great Eastern Shipping", "sector": "Shipping",      "beta": 1.20},
    {"symbol": "GILLETTE.NS",     "name": "Gillette India",         "sector": "FMCG",          "beta": 0.50},
    {"symbol": "GLAXO.NS",        "name": "GSK Pharma India",       "sector": "Pharma",        "beta": 0.60},
    {"symbol": "GLOBALHEALT.NS",  "name": "Global Health (Medanta)","sector": "Healthcare",    "beta": 1.00},
    {"symbol": "GMDCLTD.NS",      "name": "GMDC",                   "sector": "Metal",         "beta": 1.00},
    {"symbol": "GPIL.NS",         "name": "Godawari Power & Ispat", "sector": "Metal",         "beta": 1.30},
    {"symbol": "GRANULES.NS",     "name": "Granules India",         "sector": "Pharma",        "beta": 0.90},
    {"symbol": "GRAPHITE.NS",     "name": "Graphite India",         "sector": "Engineering",   "beta": 1.20},
    {"symbol": "GREAVESCOT.NS",   "name": "Greaves Cotton",         "sector": "Auto",          "beta": 0.90},
    {"symbol": "GREENPANEL.NS",   "name": "Greenpanel Industries",  "sector": "Consumer",      "beta": 1.10},
    {"symbol": "GUJALKALI.NS",    "name": "Gujarat Alkalies",       "sector": "Chemicals",     "beta": 1.00},
    {"symbol": "GUJGASLTD.NS",    "name": "Gujarat Gas",            "sector": "Energy",        "beta": 0.90},
    {"symbol": "GULFOILLUB.NS",   "name": "Gulf Oil Lubricants",    "sector": "Energy",        "beta": 0.80},
    {"symbol": "HARDWYN.NS",      "name": "Hardwyn India",          "sector": "Consumer",      "beta": 1.00},
    {"symbol": "HBLPOWER.NS",     "name": "HBL Power Systems",      "sector": "Engineering",   "beta": 1.20},
    {"symbol": "HERITGFOOD.NS",   "name": "Heritage Foods",         "sector": "FMCG",          "beta": 0.80},
    {"symbol": "HFCL.NS",         "name": "HFCL",                   "sector": "Telecom",       "beta": 1.30},
    {"symbol": "HIKAL.NS",        "name": "Hikal",                  "sector": "Chemicals",     "beta": 1.00},
    {"symbol": "HIMATSEIDE.NS",   "name": "Himatsingka Seide",      "sector": "Textiles",      "beta": 1.00},
    {"symbol": "HINDCOPPER.NS",   "name": "Hindustan Copper",       "sector": "Metal",         "beta": 1.40},
    {"symbol": "HINDPETRO.NS",    "name": "HPCL",                   "sector": "Energy",        "beta": 1.00},
    {"symbol": "HINDWARE.NS",     "name": "Hindware Home Inn",      "sector": "Consumer",      "beta": 1.00},
    {"symbol": "HOMEFIRST.NS",    "name": "Home First Finance",     "sector": "NBFC",          "beta": 1.10},
    {"symbol": "HUDCO.NS",        "name": "HUDCO",                  "sector": "NBFC",          "beta": 1.00},
    {"symbol": "IBREALEST.NS",    "name": "Indiabulls Real Estate", "sector": "Realty",        "beta": 1.50},
    {"symbol": "ICAICREDIT.NS",   "name": "ICICI Sec",              "sector": "Financial",     "beta": 1.30},
    {"symbol": "IGPL.NS",         "name": "IG Petrochemicals",      "sector": "Chemicals",     "beta": 1.10},
    {"symbol": "IIFLSEC.NS",      "name": "IIFL Securities",        "sector": "Financial",     "beta": 1.30},
    {"symbol": "IMAGICAA.NS",     "name": "Imagicaaworld Ent",      "sector": "Entertainment", "beta": 1.20},
    {"symbol": "INDIAGLYCO.NS",   "name": "India Glycols",          "sector": "Chemicals",     "beta": 1.00},
    {"symbol": "INDIACEM.NS",     "name": "India Cements",          "sector": "Cement",        "beta": 1.10},
    {"symbol": "INDIANHUME.NS",   "name": "Indian Hume Pipe",       "sector": "Engineering",   "beta": 1.00},
    {"symbol": "INGERRAND.NS",    "name": "Ingersoll Rand India",   "sector": "Engineering",   "beta": 0.80},
    {"symbol": "INTELLECT.NS",    "name": "Intellect Design Arena", "sector": "IT",            "beta": 1.20},
    {"symbol": "INTENTECH.NS",    "name": "Intentech Solutions",    "sector": "IT",            "beta": 1.10},
    {"symbol": "IOB.NS",          "name": "Indian Overseas Bank",   "sector": "Banking",       "beta": 1.30},
    {"symbol": "IPCALAB.NS",      "name": "IPCA Laboratories",      "sector": "Pharma",        "beta": 0.80},
    {"symbol": "IRB.NS",          "name": "IRB Infrastructure",     "sector": "Infra",         "beta": 1.20},
    {"symbol": "IREDA.NS",        "name": "IREDA",                  "sector": "NBFC",          "beta": 1.10},
    {"symbol": "ITC.NS",          "name": "ITC",                    "sector": "FMCG",          "beta": 0.60},
    {"symbol": "ITI.NS",          "name": "ITI",                    "sector": "Telecom",       "beta": 1.30},
    {"symbol": "J&KBANK.NS",      "name": "J&K Bank",               "sector": "Banking",       "beta": 1.20},
    {"symbol": "JAGRAN.NS",       "name": "Jagran Prakashan",       "sector": "Media",         "beta": 0.80},
    {"symbol": "JAMNAAUTO.NS",    "name": "Jamna Auto",             "sector": "Auto Ancillary","beta": 1.00},
    {"symbol": "JBCHEPHARM.NS",   "name": "JB Chemicals",           "sector": "Pharma",        "beta": 0.80},
    {"symbol": "JBMA.NS",         "name": "JBM Auto",               "sector": "Auto",          "beta": 1.20},
    {"symbol": "JINDALPOLY.NS",   "name": "Jindal Poly Films",      "sector": "Chemicals",     "beta": 1.00},
    {"symbol": "JINDALSAW.NS",    "name": "Jindal SAW",             "sector": "Metal",         "beta": 1.20},
    {"symbol": "JINDALSTEL.NS",   "name": "Jindal Steel & Power",   "sector": "Metal",         "beta": 1.40},
    {"symbol": "JKLAKSHMI.NS",    "name": "JK Lakshmi Cement",      "sector": "Cement",        "beta": 1.00},
    {"symbol": "JKIL.NS",         "name": "J Kumar Infraprojects",  "sector": "Engineering",   "beta": 1.20},
    {"symbol": "JMFINANCIL.NS",   "name": "JM Financial",           "sector": "Financial",     "beta": 1.20},
    {"symbol": "JNKIA.NS",        "name": "JNK India",              "sector": "Engineering",   "beta": 1.10},
    {"symbol": "JSL.NS",          "name": "Jindal Stainless",       "sector": "Metal",         "beta": 1.30},
    {"symbol": "JTEKTINDIA.NS",   "name": "JTEKT India",            "sector": "Auto Ancillary","beta": 0.90},
    {"symbol": "JUSTDIAL.NS",     "name": "Just Dial",              "sector": "Internet",      "beta": 1.30},
    {"symbol": "JYOTHYLAB.NS",    "name": "Jyothy Labs",            "sector": "FMCG",          "beta": 0.70},
    {"symbol": "KALPATPOWR.NS",   "name": "Kalpataru Projects",     "sector": "Engineering",   "beta": 1.10},
    {"symbol": "KANSAINER.NS",    "name": "Kansai Nerolac",         "sector": "Consumer",      "beta": 0.80},
    {"symbol": "KFINTECH.NS",     "name": "KFin Technologies",      "sector": "Financial",     "beta": 1.10},
    {"symbol": "KNRCON.NS",       "name": "KNR Constructions",      "sector": "Engineering",   "beta": 1.10},
    {"symbol": "KOKUYOCMLN.NS",   "name": "Kokuyo Camlin",          "sector": "Consumer",      "beta": 0.80},
    {"symbol": "KRBL.NS",         "name": "KRBL",                   "sector": "FMCG",          "beta": 0.80},
    {"symbol": "KSB.NS",          "name": "KSB",                    "sector": "Engineering",   "beta": 0.90},
    {"symbol": "KSCL.NS",         "name": "Kaveri Seed Company",    "sector": "Agriculture",   "beta": 0.80},
    {"symbol": "KTKBANK.NS",      "name": "Karnataka Bank",         "sector": "Banking",       "beta": 1.10},
    {"symbol": "LATENTVIEW.NS",   "name": "Latent View Analytics",  "sector": "IT",            "beta": 1.30},
    {"symbol": "LEMONTREE.NS",    "name": "Lemon Tree Hotels",      "sector": "Hotels",        "beta": 1.20},
    {"symbol": "LINDEINDIA.NS",   "name": "Linde India",            "sector": "Chemicals",     "beta": 0.80},
    {"symbol": "LLOYDSENGG.NS",   "name": "Lloyd Engineerings",     "sector": "Engineering",   "beta": 1.20},
    {"symbol": "LODHA.NS",        "name": "Macrotech Developers",   "sector": "Realty",        "beta": 1.30},
    {"symbol": "LXCHEM.NS",       "name": "LX Chemical",            "sector": "Chemicals",     "beta": 1.00},
    {"symbol": "MAHARASHTRA.NS",  "name": "Maharashtra Seamless",   "sector": "Metal",         "beta": 1.10},
    {"symbol": "MAHSEAMLES.NS",   "name": "Mah. Seamless",          "sector": "Metal",         "beta": 1.10},
    {"symbol": "MASFIN.NS",       "name": "MAS Financial Services", "sector": "NBFC",          "beta": 1.00},
    {"symbol": "MCDOWELL-N.NS",   "name": "United Spirits",         "sector": "FMCG",          "beta": 0.80},
    {"symbol": "MEDANTA.NS",      "name": "Global Health",          "sector": "Healthcare",    "beta": 1.00},
    {"symbol": "MEDPLUS.NS",      "name": "MedPlus Health",         "sector": "Healthcare",    "beta": 1.10},
    {"symbol": "MIDHANI.NS",      "name": "MIDHANI",                "sector": "Defence",       "beta": 1.00},
    {"symbol": "MINDACORP.NS",    "name": "Minda Corporation",      "sector": "Auto Ancillary","beta": 1.10},
    {"symbol": "MIRZAINT.NS",     "name": "Mirza International",    "sector": "Textiles",      "beta": 1.00},
    {"symbol": "MKPL.NS",         "name": "MK Proteins",            "sector": "FMCG",          "beta": 1.00},
    {"symbol": "MOIL.NS",         "name": "MOIL",                   "sector": "Metal",         "beta": 1.00},
    {"symbol": "MOTILALOFS.NS",   "name": "Motilal Oswal Financial","sector": "Financial",     "beta": 1.30},
    {"symbol": "MPHASIS.NS",      "name": "Mphasis",                "sector": "IT",            "beta": 1.10},
    {"symbol": "MRPL.NS",         "name": "Mangalore Refinery",     "sector": "Energy",        "beta": 1.10},
    {"symbol": "MUKANDLTD.NS",    "name": "Mukand",                 "sector": "Metal",         "beta": 1.20},
    {"symbol": "MUTHOOTFIN.NS",   "name": "Muthoot Finance",        "sector": "NBFC",          "beta": 1.10},
    {"symbol": "NACLIND.NS",      "name": "NACL Industries",        "sector": "Chemicals",     "beta": 1.00},
    {"symbol": "NATIONALUM.NS",   "name": "National Aluminium",     "sector": "Metal",         "beta": 1.20},
    {"symbol": "NAVA.NS",         "name": "Nava",                   "sector": "Power",         "beta": 1.10},
    {"symbol": "NAVINFLUOR.NS",   "name": "Navin Fluorine",         "sector": "Chemicals",     "beta": 1.00},
    {"symbol": "NAYARA.NS",       "name": "Nayara Energy",          "sector": "Energy",        "beta": 1.10},
    {"symbol": "NGL.NS",          "name": "NGL Fine-Chem",          "sector": "Chemicals",     "beta": 1.00},
    {"symbol": "NH.NS",           "name": "Narayana Hrudayalaya",   "sector": "Healthcare",    "beta": 1.00},
    {"symbol": "NIITLTD.NS",      "name": "NIIT",                   "sector": "IT",            "beta": 1.10},
    {"symbol": "NSLNISP.NS",      "name": "NMDC Steel",             "sector": "Metal",         "beta": 1.30},
    {"symbol": "NUVAMA.NS",       "name": "Nuvama Wealth",          "sector": "Financial",     "beta": 1.20},
    {"symbol": "NUVOCO.NS",       "name": "Nuvoco Vistas",          "sector": "Cement",        "beta": 1.00},
    {"symbol": "OAL.NS",          "name": "Oriental Aromatics",     "sector": "Chemicals",     "beta": 0.90},
    {"symbol": "OCCL.NS",         "name": "Oriental Carbon & Chem", "sector": "Chemicals",     "beta": 1.00},
    {"symbol": "OFSS.NS",         "name": "Oracle Fin Services",    "sector": "IT",            "beta": 0.80},
    {"symbol": "ONGC.NS",         "name": "ONGC",                   "sector": "Energy",        "beta": 1.00},
    {"symbol": "OPTIEMUS.NS",     "name": "Optiemus Infracom",      "sector": "Electronics",   "beta": 1.30},
    {"symbol": "ORIENTCEM.NS",    "name": "Orient Cement",          "sector": "Cement",        "beta": 1.00},
    {"symbol": "ORIENTELEC.NS",   "name": "Orient Electric",        "sector": "Consumer",      "beta": 1.00},
    {"symbol": "ORIENTHOTEL.NS",  "name": "Oriental Hotels",        "sector": "Hotels",        "beta": 1.10},
    {"symbol": "PATELENG.NS",     "name": "Patel Engineering",      "sector": "Engineering",   "beta": 1.20},
    {"symbol": "PATANJALI.NS",    "name": "Patanjali Foods",        "sector": "FMCG",          "beta": 0.90},
    {"symbol": "PCBL.NS",         "name": "PCBL",                   "sector": "Chemicals",     "beta": 1.10},
    {"symbol": "PDSL.NS",         "name": "PDS",                    "sector": "Textiles",      "beta": 1.00},
    {"symbol": "PENIND.NS",       "name": "Pen India",              "sector": "Media",         "beta": 1.00},
    {"symbol": "PGINV.NS",        "name": "Procter & Gamble",       "sector": "FMCG",          "beta": 0.50},
    {"symbol": "PHOENIXLTD.NS",   "name": "Phoenix Mills",          "sector": "Realty",        "beta": 1.20},
    {"symbol": "PILANIINVS.NS",   "name": "Pilani Investment",      "sector": "Conglomerate",  "beta": 0.90},
    {"symbol": "PNBHOUSING.NS",   "name": "PNB Housing Finance",    "sector": "NBFC",          "beta": 1.20},
    {"symbol": "POLYMED.NS",      "name": "Poly Medicure",          "sector": "Healthcare",    "beta": 0.90},
    {"symbol": "POONAWALLA.NS",   "name": "Poonawalla Fincorp",     "sector": "NBFC",          "beta": 1.20},
    {"symbol": "POWERMECH.NS",    "name": "Power Mech Projects",    "sector": "Engineering",   "beta": 1.20},
    {"symbol": "PREMIEREXP.NS",   "name": "Premier Explosives",     "sector": "Defence",       "beta": 1.20},
    {"symbol": "PRIMEFOCUS.NS",   "name": "Prime Focus",            "sector": "Media",         "beta": 1.30},
    {"symbol": "PRINCEPIPE.NS",   "name": "Prince Pipes",           "sector": "Consumer",      "beta": 1.00},
    {"symbol": "PRSMJOHNSN.NS",   "name": "Prism Johnson",          "sector": "Cement",        "beta": 1.00},
    {"symbol": "PRIVISCL.NS",     "name": "Privi Speciality Chem",  "sector": "Chemicals",     "beta": 1.00},
    {"symbol": "PSPPROJECT.NS",   "name": "PSP Projects",           "sector": "Engineering",   "beta": 1.10},
    {"symbol": "PURVA.NS",        "name": "Puravankara",            "sector": "Realty",        "beta": 1.30},
    {"symbol": "QUESS.NS",        "name": "Quess Corp",             "sector": "Services",      "beta": 1.10},
    {"symbol": "RADICO.NS",       "name": "Radico Khaitan",         "sector": "FMCG",          "beta": 0.90},
    {"symbol": "RAIL VIKAS.NS",   "name": "Rail Vikas Nigam",       "sector": "Engineering",   "beta": 1.10},
    {"symbol": "RAILTEL.NS",      "name": "RailTel Corporation",    "sector": "Telecom",       "beta": 1.10},
    {"symbol": "RAIN.NS",         "name": "Rain Industries",        "sector": "Chemicals",     "beta": 1.30},
    {"symbol": "RAJRATAN.NS",     "name": "Rajratan Global Wire",   "sector": "Metal",         "beta": 1.10},
    {"symbol": "RALLIS.NS",       "name": "Rallis India",           "sector": "Chemicals",     "beta": 0.90},
    {"symbol": "RAMCOIND.NS",     "name": "Ramco Industries",       "sector": "Cement",        "beta": 0.90},
    {"symbol": "RATNAMANI.NS",    "name": "Ratnamani Metals",       "sector": "Metal",         "beta": 1.00},
    {"symbol": "RAYMOND.NS",      "name": "Raymond",                "sector": "Textiles",      "beta": 1.10},
    {"symbol": "RBLBANK.NS",      "name": "RBL Bank",               "sector": "Banking",       "beta": 1.40},
    {"symbol": "REDINGTON.NS",    "name": "Redington",              "sector": "IT",            "beta": 0.90},
    {"symbol": "RESPONIND.NS",    "name": "Responsive Industries",  "sector": "Consumer",      "beta": 1.10},
    {"symbol": "RKFORGE.NS",      "name": "Ramkrishna Forgings",    "sector": "Auto Ancillary","beta": 1.10},
    {"symbol": "ROLCON.NS",       "name": "Rolcon Engineering",     "sector": "Engineering",   "beta": 1.00},
    {"symbol": "ROSSARI.NS",      "name": "Rossari Biotech",        "sector": "Chemicals",     "beta": 1.00},
    {"symbol": "ROUTE.NS",        "name": "Route Mobile",           "sector": "IT",            "beta": 1.20},
    {"symbol": "RVNL.NS",         "name": "Rail Vikas Nigam",       "sector": "Engineering",   "beta": 1.10},
    {"symbol": "SAFARI.NS",       "name": "Safari Industries",      "sector": "Consumer",      "beta": 1.10},
    {"symbol": "SAKSOFT.NS",      "name": "Saksoft",                "sector": "IT",            "beta": 1.00},
    {"symbol": "SANDESH.NS",      "name": "Sandesh",                "sector": "Media",         "beta": 0.80},
    {"symbol": "SANGHIIND.NS",    "name": "Sanghi Industries",      "sector": "Cement",        "beta": 1.00},
    {"symbol": "SARDAEN.NS",      "name": "Sarda Energy & Minerals","sector": "Metal",         "beta": 1.20},
    {"symbol": "SAREGAMA.NS",     "name": "Saregama India",         "sector": "Media",         "beta": 1.00},
    {"symbol": "SCHAEFFLER.NS",   "name": "Schaeffler India",       "sector": "Auto Ancillary","beta": 0.90},
    {"symbol": "SEQUENT.NS",      "name": "SeQuent Scientific",     "sector": "Pharma",        "beta": 1.00},
    {"symbol": "SHARDACROP.NS",   "name": "Sharda Cropchem",        "sector": "Chemicals",     "beta": 0.90},
    {"symbol": "SHRIRAMCIT.NS",   "name": "Shriram City Union",     "sector": "NBFC",          "beta": 1.10},
    {"symbol": "SHYAMMETL.NS",    "name": "Shyam Metalics",         "sector": "Metal",         "beta": 1.20},
    {"symbol": "SINTERCOM.NS",    "name": "Sintercom India",        "sector": "Auto Ancillary","beta": 1.00},
    {"symbol": "SKIPPER.NS",      "name": "Skipper",                "sector": "Engineering",   "beta": 1.10},
    {"symbol": "SMLISUZU.NS",     "name": "SML Isuzu",              "sector": "Auto",          "beta": 1.10},
    {"symbol": "SNOWMAN.NS",      "name": "Snowman Logistics",      "sector": "Logistics",     "beta": 1.00},
    {"symbol": "SOLARA.NS",       "name": "Solara Active Pharma",   "sector": "Pharma",        "beta": 1.00},
    {"symbol": "SOUTHBANK.NS",    "name": "South Indian Bank",      "sector": "Banking",       "beta": 1.20},
    {"symbol": "SPDL.NS",         "name": "Sundaram-Clayton",       "sector": "Auto Ancillary","beta": 1.00},
    {"symbol": "SPLPETRO.NS",     "name": "Selan Exploration",      "sector": "Energy",        "beta": 1.10},
    {"symbol": "SPMLINFRA.NS",    "name": "SPML Infra",             "sector": "Engineering",   "beta": 1.20},
    {"symbol": "SREEL.NS",        "name": "Sreeleathers",           "sector": "Consumer",      "beta": 0.90},
    {"symbol": "SRTRANSFIN.NS",   "name": "Shriram Transport Fin",  "sector": "NBFC",          "beta": 1.20},
    {"symbol": "STOVEKRAFT.NS",   "name": "Stove Kraft",            "sector": "Consumer",      "beta": 1.00},
    {"symbol": "SUBROS.NS",       "name": "Subros",                 "sector": "Auto Ancillary","beta": 1.00},
    {"symbol": "SUDARSCHEM.NS",   "name": "Sudarshan Chemical",     "sector": "Chemicals",     "beta": 0.90},
    {"symbol": "SUMIT.NS",        "name": "Sumit Woods",            "sector": "Realty",        "beta": 1.20},
    {"symbol": "SUNFLAG.NS",      "name": "Sunflag Iron",           "sector": "Metal",         "beta": 1.10},
    {"symbol": "SUPRAJIT.NS",     "name": "Suprajit Engineering",   "sector": "Auto Ancillary","beta": 0.90},
    {"symbol": "SUPRIYA.NS",      "name": "Supriya Lifescience",    "sector": "Pharma",        "beta": 1.00},
    {"symbol": "SUVEN.NS",        "name": "Suven Pharmaceuticals",  "sector": "Pharma",        "beta": 1.00},
    {"symbol": "SUZLON.NS",       "name": "Suzlon Energy",          "sector": "Power",         "beta": 1.60},
    {"symbol": "SWSOLAR.NS",      "name": "Sterling & Wilson Solar","sector": "Power",         "beta": 1.40},
    {"symbol": "SYMPHONY.NS",     "name": "Symphony",               "sector": "Consumer",      "beta": 0.90},
    {"symbol": "TANLA.NS",        "name": "Tanla Platforms",        "sector": "IT",            "beta": 1.30},
    {"symbol": "TASTYBITE.NS",    "name": "Tasty Bite Eatables",    "sector": "FMCG",          "beta": 0.70},
    {"symbol": "TATAINVEST.NS",   "name": "Tata Investment Corp",   "sector": "Conglomerate",  "beta": 0.90},
    {"symbol": "TATAMETALI.NS",   "name": "Tata Metaliks",          "sector": "Metal",         "beta": 1.20},
    {"symbol": "TBOTEK.NS",       "name": "TBO Tek",                "sector": "Internet",      "beta": 1.30},
    {"symbol": "TCNSBRANDS.NS",   "name": "TCNS Clothing",          "sector": "Retail",        "beta": 1.10},
    {"symbol": "TDPOWERSYS.NS",   "name": "TD Power Systems",       "sector": "Engineering",   "beta": 1.00},
    {"symbol": "TEJASNET.NS",     "name": "Tejas Networks",         "sector": "Telecom",       "beta": 1.30},
    {"symbol": "THERMAX.NS",      "name": "Thermax",                "sector": "Engineering",   "beta": 0.90},
    {"symbol": "THYROCARE.NS",    "name": "Thyrocare",              "sector": "Healthcare",    "beta": 0.80},
    {"symbol": "TIINDIA.NS",      "name": "Tube Investments",       "sector": "Auto Ancillary","beta": 1.10},
    {"symbol": "TINPLATE.NS",     "name": "Tinplate Company",       "sector": "Metal",         "beta": 1.00},
    {"symbol": "TIPSINDLTD.NS",   "name": "Tips Industries",        "sector": "Media",         "beta": 1.00},
    {"symbol": "TITAN.NS",        "name": "Titan Company",          "sector": "Consumer",      "beta": 1.10},
    {"symbol": "TORNTPOWER.NS",   "name": "Torrent Power",          "sector": "Power",         "beta": 0.80},
    {"symbol": "TRIL.NS",         "name": "Trident",                "sector": "Textiles",      "beta": 1.10},
    {"symbol": "TRITURBINE.NS",   "name": "Triveni Turbine",        "sector": "Engineering",   "beta": 1.00},
    {"symbol": "TTKPRESTIG.NS",   "name": "TTK Prestige",           "sector": "Consumer",      "beta": 0.80},
    {"symbol": "UJJIVANSFB.NS",   "name": "Ujjivan Small Fin Bank", "sector": "Banking",       "beta": 1.20},
    {"symbol": "UMBRELLA.NS",     "name": "Umbrella Infocare",      "sector": "IT",            "beta": 1.00},
    {"symbol": "UNIENTER.NS",     "name": "Unison Enviro",          "sector": "Engineering",   "beta": 1.00},
    {"symbol": "UNIVCABLES.NS",   "name": "Universal Cables",       "sector": "Engineering",   "beta": 1.00},
    {"symbol": "USHDEV.NS",       "name": "Ush Dev International",  "sector": "Textiles",      "beta": 1.00},
    {"symbol": "UTKARSHBNK.NS",   "name": "Utkarsh Small Fin Bank", "sector": "Banking",       "beta": 1.20},
    {"symbol": "V2RETAIL.NS",     "name": "V2 Retail",              "sector": "Retail",        "beta": 1.20},
    {"symbol": "VAIBHAVGBL.NS",   "name": "Vaibhav Global",         "sector": "Retail",        "beta": 1.00},
    {"symbol": "VAKRANGEE.NS",    "name": "Vakrangee",              "sector": "IT",            "beta": 1.40},
    {"symbol": "VALIANTORG.NS",   "name": "Valiant Organics",       "sector": "Chemicals",     "beta": 1.00},
    {"symbol": "VARDHACRLC.NS",   "name": "Vardhman Special Steels","sector": "Metal",         "beta": 1.10},
    {"symbol": "VARROC.NS",       "name": "Varroc Engineering",     "sector": "Auto Ancillary","beta": 1.20},
    {"symbol": "VBL.NS",          "name": "Varun Beverages",        "sector": "FMCG",          "beta": 0.90},
    {"symbol": "VEDL.NS",         "name": "Vedanta",                "sector": "Metal",         "beta": 1.50},
    {"symbol": "VERANDA.NS",      "name": "Veranda Learning Sol",   "sector": "Education",     "beta": 1.20},
    {"symbol": "VIJAYA.NS",       "name": "Vijaya Diagnostic",      "sector": "Healthcare",    "beta": 0.90},
    {"symbol": "VINATIORGA.NS",   "name": "Vinati Organics",        "sector": "Chemicals",     "beta": 0.90},
    {"symbol": "VIPIND.NS",       "name": "VIP Industries",         "sector": "Consumer",      "beta": 1.00},
    {"symbol": "VMART.NS",        "name": "V-Mart Retail",          "sector": "Retail",        "beta": 1.10},
    {"symbol": "VOLTAMP.NS",      "name": "Voltamp Transformers",   "sector": "Engineering",   "beta": 1.00},
    {"symbol": "VOLTAS.NS",       "name": "Voltas",                 "sector": "Consumer",      "beta": 1.00},
    {"symbol": "WABAG.NS",        "name": "VA Tech Wabag",          "sector": "Engineering",   "beta": 1.10},
    {"symbol": "WEBELSOLAR.NS",   "name": "Webel Solar",            "sector": "Power",         "beta": 1.30},
    {"symbol": "WELCORP.NS",      "name": "Welspun Corp",           "sector": "Metal",         "beta": 1.20},
    {"symbol": "WELENT.NS",       "name": "Welspun Enterprises",    "sector": "Engineering",   "beta": 1.20},
    {"symbol": "WELSPUNIND.NS",   "name": "Welspun India",          "sector": "Textiles",      "beta": 1.10},
    {"symbol": "WESTLIFE.NS",     "name": "Westlife Foodworld",     "sector": "Retail",        "beta": 1.10},
    {"symbol": "WINDMACHIN.NS",   "name": "Windsor Machines",       "sector": "Engineering",   "beta": 1.00},
    {"symbol": "XCHANGING.NS",    "name": "Xchanging Solutions",    "sector": "IT",            "beta": 1.00},
    {"symbol": "YATHARTH.NS",     "name": "Yatharth Hospital",      "sector": "Healthcare",    "beta": 1.10},
    {"symbol": "ZAUBACORP.NS",    "name": "Zauba Corp",             "sector": "IT",            "beta": 1.00},
    {"symbol": "ZENTEC.NS",       "name": "Zen Technologies",       "sector": "Defence",       "beta": 1.30},
    {"symbol": "ZFCVINDIA.NS",    "name": "ZF Commercial Vehicle",  "sector": "Auto Ancillary","beta": 0.90},
    {"symbol": "ZIMLAB.NS",       "name": "Zim Laboratories",       "sector": "Pharma",        "beta": 1.00},
    {"symbol": "ZOMATO.NS",       "name": "Zomato",                 "sector": "Internet",      "beta": 1.60},
    {"symbol": "ZUARI.NS",        "name": "Zuari Agro Chemicals",   "sector": "Chemicals",     "beta": 1.00},
    {"symbol": "ZYDUSLIFE.NS",    "name": "Zydus Lifesciences",     "sector": "Pharma",        "beta": 0.80},
    {"symbol": "ZYDUSWELL.NS",    "name": "Zydus Wellness",         "sector": "FMCG",          "beta": 0.80},
]

# Deduplicate by symbol
seen = set()
WATCHLIST_CLEAN = []
for item in WATCHLIST:
    if item["symbol"] not in seen:
        seen.add(item["symbol"])
        WATCHLIST_CLEAN.append(item)
WATCHLIST = WATCHLIST_CLEAN

# ── Sector data ──────────────────────────────────────────────────────────────

SECTOR_PE = {
    "Banking": 18, "Pharma": 28, "Auto": 20, "Energy": 12,
    "FMCG": 50, "Infra": 22, "Power": 18, "IT": 25,
    "Consumer": 55, "NBFC": 22, "Conglomerate": 20, "Metal": 14,
    "Cement": 20, "Chemicals": 30, "Healthcare": 35, "Insurance": 40,
    "Realty": 25, "Telecom": 22, "Engineering": 24, "Defence": 30,
    "Auto Ancillary": 22, "Logistics": 28, "Internet": 60, "Fintech": 50,
    "Retail": 55, "Media": 18, "Textiles": 15, "Financial": 25,
    "Agriculture": 18, "Hotels": 30, "Electronics": 35, "Services": 25,
    "Shipping": 10, "Exchange": 35, "Paper": 12, "Education": 40,
    "Entertainment": 30, "Aviation": 20, "Travel": 22, "Trading": 12,
}

SECTOR_OUTLOOK = {
    "Banking":       ("Positive", "RBI rate-cut cycle benefits NIMs; credit growth robust; FII re-entry expected."),
    "Pharma":        ("Strong",   "Weak rupee boosts USD export earnings; sector outperforming in volatile markets."),
    "Auto":          ("Positive", "GST reforms positive for auto; above-normal monsoon strengthens rural demand."),
    "Energy":        ("Stable",   "High crude benefits domestic coal/gas substitution; power demand rising."),
    "FMCG":          ("Positive", "Above-normal monsoon drives rural volumes; income-tax relief lifts urban spend."),
    "Infra":         ("Positive", "Govt capex thrust; PM Modi diplomatic visits catalysing trade-corridor deals."),
    "Power":         ("Strong",   "India power demand surging — summer heat, data-centre expansion, EV growth."),
    "IT":            ("Cautious", "Sector beaten down — contrarian opportunity; weak rupee boosts USD revenue."),
    "Consumer":      ("Positive", "Income-tax relief drives discretionary spending; wedding/festive season ahead."),
    "NBFC":          ("Positive", "Credit growth robust; rate-cut cycle boosts cost of funds outlook."),
    "Conglomerate":  ("Stable",   "Diversified business mix provides resilience; domestic capex beneficiaries."),
    "Metal":         ("Cautious", "China slowdown weighs on global metals; domestic infra spend is a tailwind."),
    "Cement":        ("Positive", "Govt infra push drives demand; consolidation in sector benefits larger players."),
    "Chemicals":     ("Positive", "China+1 strategy driving exports; specialty chemicals in strong demand globally."),
    "Healthcare":    ("Positive", "Domestic healthcare spending rising; hospital chains seeing strong occupancy."),
    "Insurance":     ("Positive", "Under-penetrated market; rising awareness post-COVID drives premium growth."),
    "Realty":        ("Strong",   "Housing demand at multi-year highs; commercial real estate recovering strongly."),
    "Telecom":       ("Positive", "ARPU improvement cycle; 5G rollout driving capex but also future revenue."),
    "Engineering":   ("Positive", "Order book at record highs; govt capex in railways, defence, roads benefitting."),
    "Defence":       ("Strong",   "Indigenisation push; record defence budget; export orders rising significantly."),
    "Auto Ancillary":("Positive", "EV transition and auto production recovery driving component demand."),
    "Logistics":     ("Positive", "Formalisation of logistics sector; GST and e-commerce driving volume growth."),
    "Internet":      ("Cautious", "Profitable internet names attractive; profitability path key to re-rating."),
    "Fintech":       ("Neutral",  "Regulatory scrutiny elevated; UPI growth positive; profitability still evolving."),
    "Retail":        ("Positive", "Organised retail gaining share; premiumisation trend strong in urban India."),
    "Media":         ("Neutral",  "OTT disruption continues; regional media more resilient than national."),
    "Textiles":      ("Cautious", "China+1 opportunity but execution has been slow; cotton prices volatile."),
    "Financial":     ("Positive", "Capital markets buoyant; wealth management and broking seeing strong growth."),
    "Agriculture":   ("Positive", "Above-normal monsoon forecast; govt support for agri sector positive."),
    "Hotels":        ("Strong",   "Tourism boom; occupancy and ARR at decade highs; strong domestic travel."),
    "Electronics":   ("Positive", "PLI scheme driving manufacturing; Apple supply chain diversification to India."),
    "Services":      ("Neutral",  "Mixed; staffing firms seeing demand from IT and manufacturing sectors."),
    "Shipping":      ("Positive", "Freight rates stabilising; fleet expansion underway; Red Sea disruption impact."),
    "Exchange":      ("Positive", "Capital market volumes at highs; derivatives segment robust."),
    "Paper":         ("Neutral",  "Packaging demand steady; wood pulp prices impacting margins."),
    "Education":     ("Positive", "EdTech consolidation; offline education robust; premium coaching demand strong."),
    "Entertainment": ("Neutral",  "OTT + multiplex recovering; content quality key to sustained recovery."),
    "Aviation":      ("Cautious", "Capacity expansion underway; fuel costs and rupee are key risks."),
    "Travel":        ("Positive", "Domestic tourism at highs; IRCTC monopoly advantage intact."),
    "Trading":       ("Neutral",  "Commodity trading margins thin; watch for regulatory changes."),
}


def is_market_open():
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    mo = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    mc = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return mo <= now <= mc


# ── Technical indicators ─────────────────────────────────────────────────────

def ema(data, span):
    k, r = 2 / (span + 1), [data[0]]
    for p in data[1:]:
        r.append(p * k + r[-1] * (1 - k))
    return np.array(r)

def compute_rsi(prices, period=14):
    if len(prices) < period + 1:
        return None
    d = np.diff(prices)
    g, l = np.where(d > 0, d, 0.0), np.where(d < 0, -d, 0.0)
    ag, al = np.mean(g[:period]), np.mean(l[:period])
    for i in range(period, len(g)):
        ag = (ag * (period - 1) + g[i]) / period
        al = (al * (period - 1) + l[i]) / period
    return round(100 - 100 / (1 + ag / al) if al else 100, 2)

def compute_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow + signal:
        return None, None, None
    ef, es = ema(prices, fast), ema(prices, slow)
    ml = ef[-len(es):] - es
    sl = ema(ml, signal)
    hist = ml[-len(sl):] - sl
    return round(float(ml[-1]), 4), round(float(sl[-1]), 4), round(float(hist[-1]), 4)

def compute_bollinger(prices, period=20, num_std=2):
    if len(prices) < period:
        return None, None, None
    w = np.array(prices[-period:])
    m, s = np.mean(w), np.std(w)
    return round(m - num_std * s, 2), round(m, 2), round(m + num_std * s, 2)

def compute_atr(highs, lows, closes, period=14):
    if len(closes) < period + 1:
        return None
    trs = [max(highs[i] - lows[i],
               abs(highs[i] - closes[i-1]),
               abs(lows[i]  - closes[i-1]))
           for i in range(1, len(closes))]
    return round(np.mean(trs[-period:]), 2)

def compute_stoch_rsi(prices, period=14):
    rsis = []
    for i in range(period, len(prices)):
        r = compute_rsi(prices[i - period:i + 1])
        if r is not None:
            rsis.append(r)
    if len(rsis) < 3:
        return None
    lo, hi = min(rsis[-period:]), max(rsis[-period:])
    return round((rsis[-1] - lo) / (hi - lo) * 100, 1) if hi != lo else 50


# ── Scoring engine ───────────────────────────────────────────────────────────

def score_stock(d):
    score, reasons = 0, []

    rsi = d.get("rsi")
    if rsi is not None:
        if rsi < 30:   score += 3; reasons.append(f"RSI {rsi} — deeply oversold, strong bounce potential")
        elif rsi < 45: score += 2; reasons.append(f"RSI {rsi} — oversold, bullish setup forming")
        elif rsi < 60: score += 1; reasons.append(f"RSI {rsi} — neutral with upward bias")
        elif rsi > 75: score -= 2; reasons.append(f"RSI {rsi} — overbought, caution warranted")
        else:          score -= 1; reasons.append(f"RSI {rsi} — slightly elevated")

    hist = d.get("macd_hist")
    macd_v, sig_v = d.get("macd"), d.get("macd_signal")
    if hist is not None:
        if hist > 0 and macd_v > sig_v:   score += 2; reasons.append("MACD bullish crossover — momentum turning positive")
        elif hist > 0:                     score += 1; reasons.append("MACD histogram positive — mild bullish momentum")
        elif hist < 0 and macd_v < sig_v:  score -= 2; reasons.append("MACD bearish — downward momentum dominant")
        else:                              score -= 1; reasons.append("MACD histogram negative — caution")

    c = d["current_price"]
    ma20, ma50, ma200 = d["ma20"], d["ma50"], d["ma200"]
    if c > ma50 > ma200:   score += 3; reasons.append("Price above MA50 & MA200 — confirmed uptrend")
    elif c > ma50:         score += 2; reasons.append("Price above MA50 — medium-term bullish")
    elif c > ma200:        score += 1; reasons.append("Price above MA200 — long-term support holding")
    elif c < ma50 < ma200: score -= 2; reasons.append("Price below MA50 & MA200 — downtrend in play")
    if ma20 > ma50:        score += 1; reasons.append("MA20 above MA50 — short-term acceleration")

    vr = d.get("volume_ratio", 1)
    if vr > 2.0:   score += 2; reasons.append(f"Volume {vr}x avg — strong institutional interest")
    elif vr > 1.3: score += 1; reasons.append(f"Volume {vr}x avg — above-average participation")
    elif vr < 0.5: score -= 1; reasons.append("Low volume — weak conviction")

    bb_l, bb_m, bb_h = d.get("bb_low"), d.get("bb_mid"), d.get("bb_high")
    if bb_l and bb_h:
        if c < bb_l:   score += 2; reasons.append("Price below lower Bollinger Band — mean-reversion buy signal")
        elif c > bb_h: score -= 1; reasons.append("Price above upper Bollinger Band — stretched, wait for pullback")
        elif c < bb_m: score += 1; reasons.append("Price below Bollinger midline — room to expand upward")

    pct = d.get("pct_from_52w_high", 0)
    if pct < -30:   score += 2; reasons.append(f"Stock is {abs(pct)}% below 52-week high — deep value zone")
    elif pct < -15: score += 1; reasons.append(f"Stock is {abs(pct)}% below 52-week high — discounted entry")
    elif pct > -5:  score -= 1; reasons.append("Near 52-week high — limited upside headroom short term")

    iv_status = d.get("iv_status")
    mos = d.get("margin_of_safety")
    if iv_status == "Undervalued":
        score += 3 if (mos or 0) > 20 else 2
        reasons.append(f"Intrinsic value ₹{d.get('intrinsic_value')} — stock undervalued by {mos}%")
    elif iv_status == "Overvalued":
        score -= 2
        reasons.append(f"Trading above intrinsic value by {abs(mos or 0)}% — valuation risk")

    return score, reasons


# ── Recommendation builder ───────────────────────────────────────────────────

def build_recommendation(d, rank):
    score, tech_reasons = score_stock(d)
    c = d["current_price"]

    if score >= 6:    signal, confidence = "BUY",  min(95, 65 + score * 3)
    elif score >= 3:  signal, confidence = "BUY",  min(75, 55 + score * 3)
    elif score <= -4: signal, confidence = "SELL", min(80, 55 + abs(score) * 3)
    else:             signal, confidence = "HOLD", 50

    atr = d.get("atr") or (c * 0.015)
    if signal == "BUY":
        target = round(c + atr * 6, 2)
        sl     = round(c - atr * 3, 2)
        upside = round((target - c) / c * 100, 1)
    elif signal == "SELL":
        target = round(c - atr * 5, 2)
        sl     = round(c + atr * 3, 2)
        upside = round((c - target) / c * 100, 1)
    else:
        target = round(c * 1.06, 2)
        sl     = round(c * 0.94, 2)
        upside = 6.0

    iv  = d.get("intrinsic_value")
    mos = d.get("margin_of_safety")
    iv_str = f"₹{iv} ({d.get('iv_status')}; {mos:+.1f}% vs CMP)" if iv else "Data unavailable"

    rsi    = d.get("rsi")
    macd_h = d.get("macd_hist")
    tech_sum = (
        f"RSI at {rsi} ({'oversold' if rsi and rsi < 40 else 'overbought' if rsi and rsi > 70 else 'neutral'}). "
        f"MACD histogram {'positive — bullish momentum' if macd_h and macd_h > 0 else 'negative — caution'}. "
        f"Price is {d['trend'].lower()} (MA20 ₹{d['ma20']}, MA50 ₹{d['ma50']}). "
        f"Volume at {d.get('volume_ratio', 1)}x 20-day average. "
        f"Bollinger midline ₹{d.get('bb_mid')}."
    )

    sector   = d["sector"]
    fund_sum = (
        f"Sector P/E benchmark: {SECTOR_PE.get(sector, 20)}x. "
        f"Intrinsic value estimate: {iv_str}. "
        f"52-week range: ₹{d['week52_low']}–₹{d['week52_high']}; "
        f"currently {abs(d.get('pct_from_52w_high', 0))}% below year-high."
    )

    outlook_tag, outlook_text = SECTOR_OUTLOOK.get(sector, ("Neutral", "Sector-neutral macro environment."))
    sit_sum = (
        f"Sector outlook: {outlook_tag}. {outlook_text} "
        f"India macro backdrop: Above-normal monsoon (IMD), RBI rate-cut cycle, "
        f"GST reforms, rupee volatility impacts export/import sectors differently."
    )

    risk_notes = []
    if d.get("beta", 1) > 1.2: risk_notes.append("high beta stock — amplifies market swings")
    if d.get("rsi", 50) > 70:  risk_notes.append("overbought RSI — pullback risk")
    if d.get("pct_from_52w_high", -10) > -5: risk_notes.append("near 52-week high — limited upside headroom")
    risk_notes.append("crude oil / rupee volatility remains a market-wide risk")
    key_risks  = "; ".join(risk_notes[:2]).capitalize() + "."
    risk_level = "High" if d.get("beta", 1) > 1.3 else "Medium"
    holding    = "6–8 weeks" if signal in ("BUY", "SELL") else "Monitor 2–4 weeks"

    return {
        "rank": rank,
        "name": d["name"],
        "symbol": d["symbol"].replace(".NS", ""),
        "sector": d["sector"],
        "signal": signal,
        "current_price": c,
        "target_price": target,
        "stop_loss": sl,
        "intrinsic_value": iv,
        "iv_status": d.get("iv_status", "N/A"),
        "margin_of_safety": mos,
        "confidence": confidence,
        "risk_level": risk_level,
        "holding_period": holding,
        "upside_pct": upside,
        "technical_summary": tech_sum,
        "fundamental_summary": fund_sum,
        "situational_summary": sit_sum,
        "key_risks": key_risks,
        "score": score,
    }


def build_option(stock_data, rank):
    c   = stock_data["current_price"]
    rsi = stock_data.get("rsi", 50)
    mh  = stock_data.get("macd_hist", 0)
    tr  = stock_data["trend"]
    sc, _ = score_stock(stock_data)

    bullish  = sc >= 3 or (rsi and rsi < 45) or (tr == "Uptrend")
    opt_type = "CE" if bullish else "PE"

    if opt_type == "CE":
        strike = round(c / 50) * 50
        if strike <= c: strike += 50
    else:
        strike = round(c / 50) * 50
        if strike >= c: strike -= 50

    now          = datetime.now(IST)
    month_ahead  = now + timedelta(days=45)
    y, m         = month_ahead.year, month_ahead.month
    last_day     = calendar.monthrange(y, m)[1]
    last_thu     = max(d for d in range(1, last_day + 1) if datetime(y, m, d).weekday() == 3)
    expiry_str   = datetime(y, m, last_thu).strftime("%d %b %Y")

    atr           = stock_data.get("atr") or (c * 0.015)
    expected_move = round(atr * 5 / c * 100, 1)

    if opt_type == "CE":
        reasoning = (
            f"{stock_data['name']} shows a bullish setup: "
            f"RSI at {rsi} ({'oversold recovery' if rsi < 45 else 'neutral-positive'}), "
            f"MACD histogram {'turning positive' if (mh or 0) > 0 else 'improving'}. "
            f"Trend is {tr.lower()}. "
            f"Buying the {strike} CE gives leveraged upside with defined risk. "
            f"Expected move of ~{expected_move}% in underlying over 4–6 weeks. "
            f"{SECTOR_OUTLOOK.get(stock_data['sector'], ('', ''))[1]}"
        )
    else:
        reasoning = (
            f"{stock_data['name']} shows a bearish setup: "
            f"RSI at {rsi} ({'overbought' if rsi > 65 else 'weakening'}), "
            f"MACD histogram negative. Trend is {tr.lower()}. "
            f"Buying the {strike} PE gives downside participation with capped risk. "
            f"Expected move of ~{expected_move}% in underlying over 4–6 weeks. "
            f"Sector outlook: {SECTOR_OUTLOOK.get(stock_data['sector'], ('', 'Sector headwinds.'))[1]}"
        )

    return {
        "rank": rank,
        "underlying": stock_data["name"],
        "symbol": stock_data["symbol"].replace(".NS", ""),
        "option_type": opt_type,
        "strike_price": strike,
        "expiry": expiry_str,
        "current_stock_price": c,
        "strategy": "Long Call" if opt_type == "CE" else "Long Put",
        "risk_level": "High",
        "target_move_pct": expected_move,
        "max_loss": "Premium paid",
        "holding_period": "4–6 weeks",
        "reasoning": reasoning,
        "key_risks": "Time decay (theta) erodes value daily; IV crush if volatility drops; always verify live option chain before trading.",
    }


# ── Data fetcher ─────────────────────────────────────────────────────────────

def fetch_stock(item):
    try:
        tk   = yf.Ticker(item["symbol"])
        hist = tk.history(period="12mo", interval="1d")
        if hist.empty or len(hist) < 30:
            return None

        closes  = hist["Close"].tolist()
        highs   = hist["High"].tolist()
        lows    = hist["Low"].tolist()
        volumes = hist["Volume"].tolist()
        c = round(closes[-1], 2)

        ma20  = round(np.mean(closes[-20:]), 2)
        ma50  = round(np.mean(closes[-50:]) if len(closes) >= 50 else np.mean(closes), 2)
        ma200 = round(np.mean(closes[-200:]) if len(closes) >= 200 else np.mean(closes), 2)

        rsi              = compute_rsi(closes)
        macd, macd_sig, macd_hist = compute_macd(closes)
        bb_l, bb_m, bb_h = compute_bollinger(closes)
        atr              = compute_atr(highs, lows, closes)

        avg_vol   = np.mean(volumes[-20:]) if len(volumes) >= 20 else np.mean(volumes)
        vol_ratio = round(volumes[-1] / avg_vol, 2) if avg_vol else 1

        w52h = max(closes[-252:]) if len(closes) >= 252 else max(closes)
        w52l = min(closes[-252:]) if len(closes) >= 252 else min(closes)

        trend = ("Uptrend"   if c > ma50 > ma200 else
                 "Downtrend" if c < ma50 < ma200 else "Sideways")

        iv_data = {"intrinsic_value": None, "margin_of_safety": None, "iv_status": "N/A"}
        try:
            info = tk.info
            eps  = info.get("trailingEps") or info.get("forwardEps")
            if eps and eps > 0:
                fair_pe = SECTOR_PE.get(item["sector"], 22)
                iv_est  = round(eps * fair_pe, 2)
                mos     = round((iv_est - c) / c * 100, 1)
                status  = ("Undervalued" if mos > 10 else
                           "Overvalued"  if mos < -10 else "Fair Value")
                iv_data = {"intrinsic_value": iv_est, "margin_of_safety": mos, "iv_status": status}
        except Exception:
            pass

        return {
            **item,
            "current_price": c,
            "ma20": ma20, "ma50": ma50, "ma200": ma200,
            "rsi": rsi,
            "macd": macd, "macd_signal": macd_sig, "macd_hist": macd_hist,
            "bb_low": bb_l, "bb_mid": bb_m, "bb_high": bb_h,
            "atr": atr,
            "volume": int(volumes[-1]),
            "avg_volume_20d": int(avg_vol),
            "volume_ratio": vol_ratio,
            "week52_high": round(w52h, 2),
            "week52_low":  round(w52l, 2),
            "pct_from_52w_high": round((c - w52h) / w52h * 100, 1),
            "trend": trend,
            **iv_data,
        }
    except Exception as e:
        logger.warning(f"Skipping {item['symbol']}: {e}")
        return None


# ── Market summary ────────────────────────────────────────────────────────────

def generate_market_summary(stocks):
    buys     = sum(1 for s in stocks if s.get("signal") == "BUY")
    sells    = sum(1 for s in stocks if s.get("signal") == "SELL")
    rsi_vals = [s["rsi"] for s in stocks if s.get("rsi")]
    avg_rsi  = round(np.mean(rsi_vals), 1) if rsi_vals else 50
    up_trend = sum(1 for s in stocks if s.get("trend") == "Uptrend")
    tone     = ("broadly bullish" if buys > sells * 2 else
                "mixed with selective opportunities" if buys >= sells else
                "cautious — risk-off mode")
    return (
        f"Market is {tone} across the {len(stocks)} stocks analysed. "
        f"{buys} BUY signals, {sells} SELL signals. "
        f"Average RSI: {avg_rsi} ({'oversold' if avg_rsi < 40 else 'overbought' if avg_rsi > 65 else 'neutral'}). "
        f"{up_trend} of {len(stocks)} stocks in confirmed uptrends. "
        f"Key macro watch: RBI rate cycle, crude oil levels, and rupee trajectory."
    )


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    try:
        return render_template("index.html")
    except Exception:
        return "<h1>StockSage Backend Active</h1><p>API: <a href='/api/recommendations'>/api/recommendations</a></p>"


@app.route("/api/market-status")
def market_status():
    now   = datetime.now(IST)
    open_ = is_market_open()
    nxt   = None
    if not open_:
        d = now + timedelta(days=1)
        while d.weekday() >= 5:
            d += timedelta(days=1)
        nxt = d.replace(hour=9, minute=15).strftime("%d %b %Y, 9:15 AM IST")
    return jsonify({
        "is_open":      open_,
        "current_time": now.strftime("%d %b %Y, %I:%M:%S %p IST"),
        "next_open":    nxt,
    })


@app.route("/api/recommendations")
def recommendations():
    try:
        stocks_data = []

        # ── Phase 1: Fetch all stocks in parallel ─────────────────────────
        # max_workers=20 for speed; Yahoo Finance tolerates this well
        logger.info(f"Starting fetch for {len(WATCHLIST)} stocks...")
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = {executor.submit(fetch_stock, item): item for item in WATCHLIST}
            for future in as_completed(futures):
                result = future.result()
                if result:
                    stocks_data.append(result)

        logger.info(f"Successfully fetched {len(stocks_data)} / {len(WATCHLIST)} stocks")

        if len(stocks_data) < 10:
            return jsonify({
                "error": "Could not fetch enough market data. Yahoo Finance may be rate-limiting — wait 1 minute and retry."
            }), 500

        # ── Phase 2: Score all fetched stocks ────────────────────────────
        scored = sorted(stocks_data, key=lambda x: score_stock(x)[0], reverse=True)

        # ── Phase 3: Top 10 stocks with sector diversification ────────────
        # Ensure no more than 2 stocks from the same sector in top 10
        top10, sector_count = [], {}
        for s in scored:
            sector = s["sector"]
            if sector_count.get(sector, 0) < 2:
                top10.append(s)
                sector_count[sector] = sector_count.get(sector, 0) + 1
            if len(top10) == 10:
                break

        # Fallback: if sector diversity reduced list below 10, fill remainder
        if len(top10) < 10:
            remaining = [s for s in scored if s not in top10]
            top10.extend(remaining[:10 - len(top10)])

        stock_recs = [build_recommendation(s, i + 1) for i, s in enumerate(top10)]

        # ── Phase 4: Top 2 option ideas (different sectors) ───────────────
        opts_raw, seen_sectors = [], set()
        for s in scored:
            if s["sector"] not in seen_sectors or len(opts_raw) == 0:
                opts_raw.append(s)
                seen_sectors.add(s["sector"])
            if len(opts_raw) == 2:
                break
        if len(opts_raw) < 2:
            opts_raw = scored[:2]
        opt_recs = [build_option(opts_raw[i], i + 1) for i in range(len(opts_raw))]

        return jsonify({
            "generated_at":    datetime.now(IST).strftime("%d %b %Y, %I:%M %p IST"),
            "stocks_analyzed": len(stocks_data),
            "market_summary":  generate_market_summary(stock_recs),
            "stocks":          stock_recs,
            "options":         opt_recs,
        })

    except Exception as e:
        logger.error(f"Recommendations error: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
