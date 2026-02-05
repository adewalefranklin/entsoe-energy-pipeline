\# 3. Main Lambda Handler



def lambda\_handler(event, context):



    token = os.environ\["entsoe\_key"]



    # Yesterday (UTC)

    yesterday = datetime.now(timezone.utc) - timedelta(days=1)

    date\_str = yesterday.strftime("%Y-%m-%d")



    period\_start = yesterday.strftime("%Y%m%d0000")

    period\_end   = (yesterday + timedelta(days=1)).strftime("%Y%m%d0000")



    results = {}



    for zone, code in ZONES.items():



        params = {

            "securityToken": token,

            "documentType": "A44",

            "processType": "A01",

            "in\_Domain": code,

            "out\_Domain": code,

            "periodStart": period\_start,

            "periodEnd": period\_end

        }



        try:

            xml\_data = get\_xml(params)

            points = extract\_points(xml\_data)



            data = {

                "zone": zone,

                "date": date\_str,

                "points": points

            }



            key = f"entsoe/day\_ahead\_prices/{zone}/{date\_str}.json"



            s3.put\_object(

                Bucket=BUCKET,

                Key=key,

                Body=json.dumps(data),

                ContentType="application/json"

            )



            results\[zone] = f"Uploaded {len(points)} points"



        except Exception as e:

            results\[zone] = f"Error: {str(e)}"



    return {

        "statusCode": 200,

        "body": json.dumps(results, indent=2)

    }

