from flask import Flask, render_template, request
import pandas as pd
import os
from google.oauth2.credentials import Credentials
import gspread
from google.auth.transport.requests import Request
import googlemaps
from dotenv import load_dotenv
from googleapiclient.discovery import build
import base64
from email.mime.text import MIMEText
import json
from flask import session
from io import StringIO
import re
import requests
from math import radians, sin, cos, sqrt, atan2
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("America/Los_Angeles")
# Load environment variables from .env file
load_dotenv()
print("GOOGLE_MAPS_API_KEY:", os.environ.get("GOOGLE_MAPS_API_KEY"))

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY')  # <-- Add this line


SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive',
         'https://www.googleapis.com/auth/gmail.send']
gmaps = googlemaps.Client(key=os.environ.get("GOOGLE_MAPS_API_KEY"))

def authenticate_google():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            from google_auth_oauthlib.flow import InstalledAppFlow
            flow = InstalledAppFlow.from_client_secrets_file('client_secret.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds
def get_gmail_service():
    creds = authenticate_google()
    return build('gmail', 'v1', credentials=creds)

def create_message(to, subject, body_text):
    message = MIMEText(body_text, 'html')
    message['to'] = to
    message['subject'] = subject

    raw = base64.urlsafe_b64encode(message.as_bytes())
    return {'raw': raw.decode()}

def send_email(service, to, subject, body):
    message = create_message(to, subject, body)
    
    send_args = {
        'userId': 'me',
        'body': message
    }

    sent = service.users().messages().send(**send_args).execute()

def send_text(phone_num, message, first_name):
    url = "https://api.openphone.com/v1/messages"
    headers={
        "Authorization": os.environ.get("AUTHORIZATION"),
        "Content-Type":"application/json"
    }
    first_name = first_name.title()
    if first_name =='Charlie':
        payload = {
            "content": message,
            "from": "PNvnUZwoP3",
            "to":[phone_num],
            "userId":"USMZbFI72a"
        }
    elif first_name == 'Mahmoud':
        payload = {
        "content": message,
        "from": "PNaOHVFQas",
        "to":[phone_num],
        "userId":"UStOusLc0x"
    }
    elif first_name == 'Ahmed':
        payload = {
        "content": message,
        "from":  'PNVYQxBEmb',
        "to":[phone_num],
        "userId":'USNNA3aaH3'
    }
    elif first_name == 'Mohamed':
        payload = {
        "content": message,
        "from":  'PNecGwld3E',
        "to":[phone_num],
        "userId":'USkdRcH9dR'
    }
    elif first_name == 'Eissa':
        payload = {
        "content": message,
        "from":  'PNGxRJ7ziq',
        "to":[phone_num],
        "userId":'US2ADkcfr0'
    }
    response = requests.post(url,headers=headers, json = payload)
    return response

def extract_10_digit_number(phone_str):
    # Find all digits
    digits = re.findall(r'\d', phone_str)
    # Join and extract the last 10 digits (in case it includes country code)
    return '+1' + ''.join(digits)[-10:]

def extract_state(address):
    match = re.search(r'\b([A-Z]{2})\s*\d{5}\b', address)
    if match:
        return match.group(1)
    return None
def openphone_get_last10_messages(phone_number_id: str, participant_e164: str):
    url = "https://api.openphone.com/v1/messages"
    headers = {
        "Authorization": os.environ.get("AUTHORIZATION"),
    }
    params = {
        "phoneNumberId": phone_number_id,
        "participants": [participant_e164],  # MUST be array
        "maxResults": 10
    }
    r = requests.get(url, headers=headers, params=params, timeout=30)
    # Return payload even on error so you can display it in the UI
    return r.status_code, r.json() if r.headers.get("content-type","").startswith("application/json") else {"raw": r.text}

def normalize_message(m: dict):
    created_utc = pd.to_datetime(m.get("createdAt"), utc=True, errors="coerce")
    created_local = created_utc.tz_convert(LOCAL_TZ) if pd.notnull(created_utc) else pd.NaT

    return {
        "direction": m.get("direction"),
        "text": (m.get("text") or "").strip(),
        "createdAt": created_utc,
        "time": created_local.strftime("%Y-%m-%d %I:%M:%S %p") if pd.notnull(created_local) else ""
    }

# def get_data():
#     creds = authenticate_google()
#     client = gspread.authorize(creds)
#     sheet = client.open('Property Network Sheet')
#     data = sheet.worksheet("Website").get_all_records()
#     df = pd.DataFrame(data)
#     df = df[df.Address != '']
#     df['Zip Code'] = df['Zip Code'].astype(str)
#     df['full_address'] = df.Address + ', ' + df.City + ', ' + df.State + ' ' + df['Zip Code']
#     df['city_state'] = df.City + ', ' + df.State
#     df.Beds = df.Beds.astype(int)
#     df.Baths = df.Baths.astype(float)

#     data2 = sheet.worksheet("Manual").get_all_records()
#     df2 = pd.DataFrame(data2)
#     # df2 = df2[df2['Phone Number']!='']
#     df2['Zip Code'] = df2['Zip Code'].astype(str)
#     df2['Phone Number'] = df2['Phone Number'].astype(str)
#     df2['full_address'] = df2.Address  +', ' + df2.City + ', ' + df2.State + ' ' + df2['Zip Code']
#     df2['city_state'] = df2.City + ', ' + df2.State
#     df2=df2[['Name','Email Address','Phone Number','full_address','city_state']]
#     return df, df2

def get_data():
    creds = authenticate_google()
    client = gspread.authorize(creds)
    sheet = client.open('Property Network Sheet')
    data = sheet.worksheet("Manual").get_all_records(expected_headers = ['PPH Relocation Specialist','Name','Email Address','Phone Number','Address','City','State','Zip Code', 'Furnished','Beds','Baths','Pets','Cleaning Fee',
                                                                     'Non Refundable Pet Deposit', 'Pet Rent', 'Refundable Security Deposit', 'Utility Cap','Link to Property','Story','Rent','House Type','Availability','Notes'])
    df = pd.DataFrame(data)
    df = df[df.Address != '']
    df['Zip Code'] = df['Zip Code'].astype(str)
    df['City'] = df['City'].astype(str)
    df['State'] = df['State'].astype(str)
    df['Address'] = df['Address'].astype(str)
    df['full_address'] = df.Address + ', ' + df.City + ', ' + df.State + ' ' + df['Zip Code']
    df['city_state'] = df.City + ', ' + df.State
    df = df[df.Baths!='']
    df = df[df.Beds !='']
    df.Beds = df.Beds.astype(int)
    df.Baths = df.Baths.astype(float)
    df['Phone Number'] = df['Phone Number'].astype(str)
    df['City, State, Zip']= df.City + ', ' + df.State + ' ' + df['Zip Code']
    return df

def get_distance_matrix_in_batches(origins, destination, batch_size=25):
    all_rows = []
    for i in range(0, len(origins), batch_size):
        batch = origins[i:i+batch_size]
        result = gmaps.distance_matrix(
            origins=batch,
            destinations=[destination],
            mode='driving',
            units='imperial'
        )
        all_rows.extend(result['rows'])
    return all_rows

def extract_miles(row):
    try:
        return row['elements'][0]['distance']['text']
    except:
        return None    

def get_lat_lon(address):
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {'address': address, 'key': os.environ.get("GOOGLE_MAPS_API_KEY")}
    
    response = requests.get(url, params=params)
    data = response.json()

    if data['status'] == 'OK':
        location = data['results'][0]['geometry']['location']
        return location['lat'], location['lng']
    else:
        return None

def return_distance_in_miles(sub, distances):
    sub['distance'] = [extract_miles(r) for r in distances]
    errors = sub[sub.distance.isna()]
    sub=sub[sub.distance.notna()]
    sub['unit'] = sub['distance'].apply(lambda x: x.split(' ')[1])
    sub['distance'] = (
        sub['distance']
        .apply(lambda x: str(x).split(' ')[0].replace(',', ''))
        .astype(float)
    )    
    # Convert all distances to miles
    sub['distance_miles'] = sub.apply(
        lambda row: row['distance'] / 5280 if row['unit'] == 'ft' else row['distance'],
        axis=1
    )
    sub = sub.sort_values(by='distance_miles')
    return sub[['full_address', 'distance_miles','Beds','Baths','Story','House Type','Rent', 'Name','Phone Number','Email Address']], errors[['full_address','Beds','Baths']]

def pipeline(dataset, target_location, min_beds, max_beds, bath, num_outputs=25, city_state_zip_list = None, city_state_list = None):
    if city_state_zip_list is not None:
        dataset['City, State, Zip'] = dataset['City, State, Zip'].apply(lambda x: x.strip(' USA'))
        sub_df = dataset[dataset['City, State, Zip'].isin(city_state_zip_list)]
    elif city_state_list is not None:
        sub_df = dataset[dataset['city_state'].isin(city_state_list)]
    # elif state_list is not None:
    #     sub_df = dataset[dataset.State.isin(state_list)]
    sub_df = sub_df[(sub_df.Beds >= min_beds) & (sub_df.Beds<=max_beds)& (sub_df.Baths >=bath)]
    origins = sub_df['full_address'].tolist()
    distances = get_distance_matrix_in_batches(origins, target_location)
    sub_df,errors = return_distance_in_miles(sub_df, distances)
    if city_state_zip_list is None and city_state_list is None:
        return sub_df[:num_outputs],errors
    else:
        return sub_df,errors

def haversine(lat1, lon1, lat2, lon2):
    R = 3958.8  # Radius of Earth in miles (use 6371 for kilometers)
    
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    
    lat1 = radians(lat1)
    lat2 = radians(lat2)
    
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    
    return R * c

def find_city_state_list(target_location, state, dataset):
    target = get_lat_lon(target_location)
    DF = pd.DataFrame(columns =['distance','city_state'])
    for _, i in dataset[dataset.State==state].iterrows():
        distance = haversine(target[0],target[1], i['Latitude'], i['Longitude'])
        df_sub = pd.DataFrame([[distance, i['city_state']]], columns = ['distance','city_state'])
        DF = pd.concat([DF,df_sub])
    DF = DF.sort_values(by='distance')
    DF['city_state'] =DF['city_state'].apply(lambda x: x.split(', ')[0].title() + ', ' + x.split(', ')[1].upper())
    print(DF.groupby('city_state').distance.min().sort_values())
    DF =  DF[DF.distance<=20].drop_duplicates()
    output = ' - '.join(DF['city_state'].dropna().astype(str).unique())
    return output
    
@app.route('/', methods=['GET', 'POST'])
def index():
    results_html = None
    e_html = None
    city_state_output = None
    filters = None

    if request.method == 'POST':
        action = request.form.get('action')
        location = request.form.get('location')
        state = request.form.get('state')
        df = get_data()
        df_coords = df[df.Latitude !='']

        df_coords.Longitude = df_coords.Longitude.astype(float)
        df_coords.Latitude = df_coords.Latitude.astype(float)
        # df_coords = df.copy()
        # df_coords["Latitude"]  = pd.to_numeric(df_coords["Latitude"], errors="coerce")
        # df_coords["Longitude"] = pd.to_numeric(df_coords["Longitude"], errors="coerce")
        # df_coords = df_coords.dropna(subset=["Latitude", "Longitude"])
        if action == 'output_city_state':
            city_state_output = find_city_state_list(location, state, df_coords)
            print("city_state_output repr:", repr(city_state_output))
            print("length:", len(city_state_output) if city_state_output else None)
        elif action == 'find_properties':
            code = request.form.get('code')
            if code == 'RentalsPPH':
                min_beds = int(request.form['min_beds'])
                max_beds = int(request.form['max_beds'])
                baths = float(request.form['baths'])
                city_state_zip = request.form.getlist('city_state_zip')
                city_state = request.form.getlist('city_state')

                if city_state[0] == '':
                    result_df, e = pipeline(df, location, min_beds, max_beds, baths, num_outputs=100, city_state_zip_list=city_state_zip[0].split(' - '))
                elif city_state_zip[0] == '':
                    result_df, e = pipeline(df, location, min_beds, max_beds, baths, num_outputs=100, city_state_list=city_state[0].split(' - '))
                result_df= result_df.reset_index().drop('index',axis=1).reset_index()
                result_df['idx'] = result_df['index']+1
                result_df = result_df.drop('index',axis=1)
                result_df = result_df[['idx','full_address','distance_miles','Beds','Baths','Story','House Type','Rent','Name','Phone Number','Email Address']]
                # session['result_df'] = result_df.to_json(orient='records')
                results_html = result_df.to_html(classes='table table-striped', index=False)
                e_html = e.to_html(classes='table table-striped', index=False)

                filters = json.dumps({
                    "location": location,
                    "min_beds": min_beds,
                    "max_beds": max_beds,
                    "baths": baths,
                    "city_state_zip": city_state_zip,
                    "city_state": city_state
                })
                
            else:
                results_html = "<p style='color:red; font-weight:bold;'>WRONG CODE</p>"
                e_html=None
    return render_template(
        'index.html',
        results=results_html,
        errors=e_html,
        filters=filters,
        city_state_output=city_state_output
    )
    # return render_template('index.html', results=results_html, errors=e_html, city_state_output=city_state_output)



    # In send_messages()
@app.route('/return_messages', methods=['POST'])
def return_messages():
    filters = json.loads(request.form['filters'])
    df = get_data()

    # run pipeline again to get fresh data
    if filters["city_state"][0] == '':
        result_df, _ = pipeline(
            df,
            filters["location"],
            filters["min_beds"],
            filters["max_beds"],
            filters["baths"],
            num_outputs=100,
            city_state_zip_list=filters["city_state_zip"][0].split(' - ')
        )
    elif filters["city_state_zip"][0] == '':
        result_df, _ = pipeline(
            df,
            filters["location"],
            filters["min_beds"],
            filters["max_beds"],
            filters["baths"],
            num_outputs=100,
            city_state_list=filters["city_state"][0].split(' - ')
        )
    result_df = result_df.reset_index().drop('index',axis=1).reset_index()
    result_df['idx'] = result_df['index']+1
    result_df = result_df.drop('index',axis=1)
    num_miles = float(request.form['miles'])
    first_name = str(request.form['first_name'])
    indices  = str(request.form['indices'])
    print('INDICES: ', indices)
    if indices!='':
        indices_to_filter = [int(i) for i in indices.split(', ')]
    if indices!='':
        result_df = result_df[result_df.idx.isin(indices_to_filter)]
    print(result_df)

    
  
    df = get_data()

    sub = result_df[(result_df.distance_miles <=num_miles) & (result_df['Phone Number']!='')][['Phone Number']]
    sub['pn'] = sub['Phone Number'].apply(lambda x: extract_10_digit_number(x))
    sub = sub.drop_duplicates(subset='pn')

    a = result_df[(result_df.distance_miles <=num_miles) & (result_df['Phone Number']!='')]
    a['pn'] = a['Phone Number'].apply(lambda x: extract_10_digit_number(x))
    addr_map = a.groupby("pn")["full_address"].apply(lambda s: list(dict.fromkeys(s.astype(str).tolist()))).to_dict()

    if first_name =='Charlie':
        PHONE_NUMBER_ID =  "PNvnUZwoP3"
    elif first_name == 'Mahmoud':
        PHONE_NUMBER_ID =  "PNaOHVFQas"
    elif first_name == 'Ahmed':
        PHONE_NUMBER_ID = 'PNVYQxBEmb'
    elif first_name == 'Mohamed':
        PHONE_NUMBER_ID = 'PNecGwld3E'
    elif first_name == 'Eissa':
        PHONE_NUMBER_ID = 'PNGxRJ7ziq'
    # Build participant list (E.164)
    participants = []
    for raw in sub['pn'].dropna().unique():
        if raw and len(raw) == 12:  # "+1" + 10 digits
            participants.append(raw)

    # Optional: limit how many conversations to render (prevents slow UI / rate issues)
    # participants = participants[:12]  # 12 cards = 4 rows of 3
    page_size = 18
    page = int(request.form.get("page", 1))
    start = (page - 1) * page_size
    end = start + page_size
    participants_page = participants[start:end]

    total = len(participants)
    total_pages = (total + page_size - 1) // page_size

    convo_cards = []
    for p in participants_page:
        p_norm = extract_10_digit_number(p)   # NEW: normalize for lookup
        addresses = addr_map.get(p_norm, [])  # NEW: list (could be empty)

        status_code, payload = openphone_get_last10_messages(PHONE_NUMBER_ID, p)

        if status_code != 200:
            convo_cards.append({
                "title": f"{p}",
                "participant": p,
                "rep_name": first_name,
                "last_activity": "",
                "addresses": addresses,  # NEW
                "messages": [{"direction": "incoming", "text": f"{status_code}: {payload}", "time": ""}],
            })
            continue

        msgs = payload.get("data", [])
        norm = [normalize_message(m) for m in msgs]
        norm = sorted(norm, key=lambda x: x["createdAt"] if pd.notnull(x["createdAt"]) else pd.Timestamp.min)

        last_activity = ""
        if norm and pd.notnull(norm[-1]["createdAt"]):
            last_activity = pd.to_datetime(norm[-1]["createdAt"]).tz_convert(LOCAL_TZ).strftime("%b %d, %I:%M %p")

        convo_cards.append({
            "title": f"{p}",
            "participant": p,
            "rep_name": first_name,
            "last_activity": last_activity,
            "addresses": addresses,  # NEW
            "messages": norm,
        })

    # Sort conversation cards by last activity (newest first)
    def parse_last_activity(c):
        # if empty, send to bottom
        if not c["last_activity"]:
            return pd.Timestamp.min
        # best effort parsing for sorting
        return pd.Timestamp.now()

    # Render back to index with convo_cards
    # return render_template(
    #     'index.html',
    #     results=None,             # or keep the results table if you want
    #     errors=None,
    #     filters=json.dumps(filters),
    #     city_state_output=None,
    #     convo_cards=convo_cards
    # )
    return render_template(
        "index.html",
        results=None,
        errors=None,
        filters=json.dumps(filters),
        city_state_output=None,
        convo_cards=convo_cards,
        page=page,
        total_pages=total_pages,
        miles=num_miles,
        first_name=first_name
    )
@app.route("/send_follow_up", methods=["POST"])
def send_follow_up():
    first_name = str(request.form.get("first_name", "")).strip()

    # ✅ read from hidden JSON list (persisted across pages)
    selected_pns_raw = request.form.get("selected_pns", "[]")
    try:
        pns = json.loads(selected_pns_raw)
    except json.JSONDecodeError:
        pns = []

    # fallback to current page checkboxes if needed
    if not pns:
        pns = request.form.getlist("pns")

    if not pns:
        return render_template("index.html", followup_status="<div class='alert alert-warning'>No conversations selected.</div>")

    message_template = request.form.get("followup_message") or "Hi! Just wanted to follow up on our housing request above."

    results = []
    for pn in pns:
        resp = send_text(pn, message_template, first_name)
        results.append({"recipient": pn, "status_code": resp.status_code, "response": resp.text})

    followup_status = pd.DataFrame(results).to_html(classes="table table-bordered table-striped", index=False)

    return render_template("index.html", followup_status=followup_status)
# @app.route("/send_follow_up", methods=["POST"])
# def send_follow_up():
#     first_name = str(request.form.get("first_name", "")).strip()
#     pns = request.form.getlist("pns")  # list of checked phone numbers (E.164)

#     if not pns:
#         return render_template(
#             "index.html",
#             results=None,
#             errors=None,
#             filters=request.form.get("filters"),
#             city_state_output=None,
#             convo_cards=None,
#             followup_status="<div class='alert alert-warning'>No conversations selected.</div>"
#         )

#     results = []
#     for pn in pns:
#         message_template = request.form.get("followup_message") or \
#     "Hi! Just wanted to follow up on our housing request above."
#         resp = send_text(pn, message_template, first_name)

#         results.append({
#             "recipient": pn,
#             "status_code": resp.status_code if resp is not None else None,
#             "response": resp.text if resp is not None else "No response"
#         })

#     df = pd.DataFrame(results)
#     followup_status = df.to_html(classes="table table-bordered table-striped", index=False)

#     # Re-render page with status (you can also redirect if you prefer)
#     return render_template(
#         "index.html",
#         results=None,
#         errors=None,
#         filters=request.form.get("filters"),
#         city_state_output=None,
#         convo_cards=None,  # if you want to keep showing cards, pass convo_cards again
#         followup_status=followup_status
#     )
    






if __name__ == '__main__':
    # app.run(debug=True)
    # Use the PORT environment variable or default to 5000 for local testing
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)