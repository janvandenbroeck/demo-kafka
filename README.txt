# Sample application for Heroku webservice with Kafka

## Web Endpoint
Implements a "fleet management" sample app, which ingests a connected car payload, and puts it in Kafka.

## Worker
Reads the Kafka Queue and updates the postgres statistics and MongoDB history

### Bugfix

if average_consumption_l_100km > 25:
    r = requests.post(os.environ['BLOWERIO_URL'] + '/messages', data={'to': result["phone__c"], 'message': "Watch out, truck with licenseplate {} has a high fuel consumption".format(result['licenseplate__c'])})
