if average_consumption_l_100km > 25:
    r = requests.post(os.environ['BLOWERIO_URL'] + '/messages', data={'to': result["phone__c"], 'message': "Watch out, truck with licenseplate {} has a high fuel consumption".format(result['licenseplate__c'])})
