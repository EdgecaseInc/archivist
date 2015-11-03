import boto3

s3c = boto3.client('s3')
#0000_part_00 => 5
#apploaded_0000_part_00 => 6
#object_0000_part_00 => 5
#header_0000_part_00 => 15

#1414949085340954
#55bce54e9b506530b82eeea8
#urbandecay

#keys = ["august_2015/header_0000_part_00","august_2015/object_0000_part_00", "august_2015/reporting/apploaded_0000_part_00", "november/0000_part_00"]
keys = ["august_2015/reporting/apploaded_0000_part_00", "november/0000_part_00"]
for key in keys:
  print "opening %s" % key
  response = s3c.get_object(Bucket="pixelarchive", Key=key, Range='bytes=0-10485760')

  parts = key.split('/')
  last = len(parts) - 1
  outfile = parts[last]
  f = open(outfile + "-out.txt", "wb")
  total = 0
  while total < 1024 * 16 * 1024:
    bytes = response["Body"].read(1024*16)
    total = len(bytes) + total
    if len(bytes) > 0:
      f.write(bytes)
    else:
      break
  print "closing %s" % key
  f.close()


#s3c.get_object(Bucket="pixelarchive", Key="august_2015/object_0000_part_00")
#s3c.get_object(Bucket="pixelarchive", Key="august_2015/reporting/apploaded_000_part_00")