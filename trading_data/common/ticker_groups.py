import requests
from bs4 import BeautifulSoup


def download_sp500_list():
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'

    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'id': 'constituents'})
    companies = []

    for row in table.find_all('tr')[1:]:
        cols = row.find_all('td')
        ticker = cols[0].text.strip()
        company_name = cols[1].text.strip()
        gics_sector = cols[2].text.strip()
        gics_sub_industry = cols[3].text.strip()
        headquarter_location = cols[4].text.strip()
        date_added = cols[5].text.strip()
        cik = cols[6].text.strip()
        founded = cols[7].text.strip()
        company = {
            'ticker': ticker,
            'company_name': company_name,
            'gics_sector': gics_sector,
            'gics_sub_industry': gics_sub_industry,
            'headquarter_location': headquarter_location,
            'date_added': date_added,
            'cik': cik,
            'founded': founded,
        }
        companies.append(company)

    return companies
