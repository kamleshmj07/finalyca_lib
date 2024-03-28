import requests

class SMSConfig:
    def __init__(self) -> None:
        self.url = None
        self.sender_id = None
        self.is_unicode = False
        self.is_flash = False
        self.api_key = None
        self.client_id = None

    def create_url(self, mobile_number, sms_text):
        s = F"{self.url}?SenderId={self.sender_id}&SenderId={self.sender_id}&Is_Unicode={self.is_unicode}&Is_Flash={self.is_flash}&ApiKey={self.api_key}&ClientId={self.client_id}&MobileNumbers={mobile_number}&Message={sms_text}"
        return s

def send_sms(sms_config: SMSConfig, mobile_number, sms_text):
    url = sms_config.create_url(mobile_number, sms_text)
    resp = requests.get(url)
    success = False
    if resp.status_code == 200:
        j = resp.json()
        if j["ErrorCode"] == 0:
            success = True

    if not success:
        raise Exception(F"Error while sending SMS to {mobile_number}. Error: {j['ErrorDescription']}")
    return success


if __name__ == '__main__':
    config = SMSConfig()
    config.url = "http://139.99.131.165:6005/api/v2/SendSMS?"
    config.sender_id = "FNLYCA"
    config.is_unicode = False
    config.is_flash = False
    config.api_key = "71715eba-3e2a-4de2-94db-b561fd1eb7ee"
    config.client_id = "ec68328b-8d18-493e-8f12-3e24f85b140d"
    
    send_sms(config, 7900062448, "Testing is SMS is coming.") 