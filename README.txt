r = requests.post(os.environ['BLOWERIO_URL'] + '/messages', data={'to': nr, 'message': data["message"]})
