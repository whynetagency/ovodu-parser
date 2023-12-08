import boto3

s3 = boto3.client('s3')

# def upload_image(s3)

print(s3.list_buckets())

bucket_name = 'ovodu-images'
img = 'out.webp'
s3_object = 'property_images/out.webp'

s3.upload_file(img, bucket_name, s3_object)
print('done')