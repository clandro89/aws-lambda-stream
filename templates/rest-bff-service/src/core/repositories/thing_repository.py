from aws_lambda_stream.repositories import BaseEntityRepository



class ThingRepository(BaseEntityRepository):
    discriminator = 'thing'

    def get_thing(self, _id):
        response = self.get({
            'Key': {
                'pk': _id,
                'sk': self.discriminator
            }
        })
        return response.get('Item', None)
