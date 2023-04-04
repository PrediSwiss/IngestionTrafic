from google.cloud import storage
import requests
import xml.etree.ElementTree as ET
from datetime import datetime

bucket_name = "prediswiss-raw-data"
url = "https://api.opentransportdata.swiss/TDP/Soap_Datex2/Pull"

headers = {
    'Content-Type': 'text/xml; charset=utf-8',
    'Authorization': '57c5dbbbf1fe4d0001000018dcaa101c238948f0892513258029a974',
    'SOAPAction': 'http://opentransportdata.swiss/TDP/Soap_Datex2/Pull/v1/pullMeasuredData' 
}

def main():
    storage_client = storage.Client(project="prediswiss")

    buckets = storage_client.list_buckets()

    if len(list(buckets)) == 0:
        print("There is 0 buckets")
        bucket = create_bucket(bucket_name, storage_client)
    else:
        bucket = storage_client.get_bucket(bucket_name)

    now = datetime.now()
    create_blob(bucket, now.strftime("%d-%m-%Y/%H-%M"), "text/json", get_data(url=url, headers=headers))
    
def get_data(url, headers):
    payload = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:SOAP-ENC=\"http://schemas.xmlsoap.org/soap/encoding/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:tdpplv1=\"http://datex2.eu/wsdl/TDP/Soap_Datex2/Pull/v1\" xmlns:dx223=\"http://datex2.eu/schema/2/2_0\"><SOAP-ENV:Body><dx223:d2LogicalModel xsi:type=\"dx223:D2LogicalModel\" modelBaseVersion=\"2\"><dx223:exchange xsi:type=\"dx223:Exchange\"><dx223:supplierIdentification xsi:type=\"dx223:InternationalIdentifier\"><dx223:country xsi:type=\"dx223:CountryEnum\">ch</dx223:country><dx223:nationalIdentifier xsi:type=\"dx223:String\">FEDRO</dx223:nationalIdentifier></dx223:supplierIdentification></dx223:exchange><dx223:payloadPublication xsi:type=\"dx223:GenericPublication\" lang=\"en\"><dx223:publicationCreator xsi:type=\"dx223:InternationalIdentifier\"><dx223:country xsi:type=\"dx223:CountryEnum\">ch</dx223:country><dx223:nationalIdentifier xsi:type=\"dx223:String\">FEDRO</dx223:nationalIdentifier></dx223:publicationCreator><dx223:genericPublicationName xsi:type=\"dx223:String\">MeasuredDataFilter</dx223:genericPublicationName><dx223:genericPublicationExtension xsi:type=\"dx223:_GenericPublicationExtensionType\"><dx223:measuredDataFilter xsi:type=\"dx223:MeasuredDataFilter\"><dx223:measurementSiteTableReference xsi:type=\"dx223:_MeasurementSiteTableVersionedReference\" targetClass=\"MeasurementSiteTable\" id=\"OTD:TrafficData\" version=\"0\"></dx223:measurementSiteTableReference><dx223:siteRequestReference xsi:type=\"dx223:_MeasurementSiteRecordVersionedReference\" targetClass=\"MeasurementSiteRecord\" id=\"CH:+.+\" version=\"0\"></dx223:siteRequestReference></dx223:measuredDataFilter></dx223:genericPublicationExtension></dx223:payloadPublication></dx223:d2LogicalModel></SOAP-ENV:Body></SOAP-ENV:Envelope>"
    response = requests.request("POST", url, headers=headers, data=payload)

    element = ET.XML(response.text)
    ET.indent(element)

    return ET.tostring(element, encoding='unicode')

def create_bucket(name, client: storage.Client):    
    bucket = client.create_bucket(name, location="eu")

    print(f"Bucket {name} created")

    return bucket

def create_blob(root_bucket: storage.Bucket, destination_name, data_type, data):
    blob = root_bucket.blob(destination_name)
    generation_match_precondition = 0
    blob.upload_from_string(data, data_type, if_generation_match=generation_match_precondition)
    print("file created")
    
if __name__ == "__main__":
    main()