# Scraper for fazwaz.com and thailand-property.com

## How to Use:
### AWS Console:
- Log in to the AWS EC2 console.
- Click on **"Instances"** and select the instance named **"ScraperRealty"**
- In the top right, click the **"Connect"** button.
- In the EC2 Instance Connect dialog, click **"Connect"**
- Ensure the following parameters are filled by default, if not:
- Connection type: Connect using EC2 Instance Connect
- Username: ubuntu

### Linux Terminal:
- Open your terminal (commands are similar for Windows).
- Run the following command:
`ssh -i "scraperKey.pem" ubuntu@ec2-16-16-26-132.eu-north-1.compute.amazonaws.com`
  *(Make sure the scraperKey.pem is in the same folder, or provide the absolute path.)*

## Running the Scraper on AWS:
- Change directory to the project folder: `cd scraper`
- Activate the virtual environment for Python: `source env/bin/activate`
- Run the main script: `python main.py`

## Additional Notes for Modifications:
- For code comments in complex sections, refer to comments marked in the code.
- **main.py** imports all parsers.
- **base.py** contains classes and constants for parsing.
- **scraper_fazwaz.py** and **scraper_thailandproperty.py** are the actual parsers.
- watermark_resolver.py handles watermark removal _(a simple version; editing may require modifying the watermark or its mask)_

## Deploying the Application Elsewhere:
- Ensure Python is installed _(preferably version 3.11)_
- Create a virtual environment: `python -m venv env`
- Activate the virtual environment: `source env/bin/activate`
- Install required dependencies: `pip install -r requirements.txt`
- Configure AWS: `aws configure`
- Run the main script: `python main.py`

**Note:** The application should work with both earlier and future versions of Python, though conflicts may arise in extreme cases.

Feel free to reach out for further assistance or enhancements!
