#importing the python libraries
from bs4 import BeautifulSoup
import requests
from csv import writer

#opening csv and setting headers
with open('data.csv', 'w', encoding='utf8', newline='') as f:
    thewriter = writer(f)
    header = ['title', 'location', 'credit', 'name', 'date', 'star']
    thewriter.writerow(header)
    
    #creating a for loop to loop through the pages
    for number in range(1,3883):
        url = "https://au.trustpilot.com/review/afterpay.com?page="+str(number)

        # requesting page data and storing it to beautiful soup
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')

        #isolating the review block and calling it "reviews"
        reviews = soup.find_all('article', class_="paper_paper__1PY90 paper_outline__lwsUX card_card__lQWDv styles_reviewCard__hcAvl")
        
        #locating the info by using the class and element
        for review in reviews:
            title = review.find('h2', class_="typography_heading-s__f7029 typography_appearance-default__AAY17").text
            location = review.find('div', class_="typography_body-m__xgxZ_ typography_appearance-subtle__8_H2l styles_detailsIcon__Fo_ua").text
            credit = review.find('span', class_="typography_body-m__xgxZ_ typography_appearance-subtle__8_H2l").text
            name = review.find('span', class_="typography_heading-xxs__QKBS8 typography_appearance-default__AAY17").text
            date = review.find('p', class_="typography_body-m__xgxZ_ typography_appearance-default__AAY17 typography_color-black__5LYEn").text.replace('Date of experience: ', '')
            starlight = review.find('div', class_="styles_reviewHeader__iU9Px").img
            #keeping only the alt text
            star = [starlight.get('alt')]

            #print it to csv
            info =  [title,location,credit,name,date,star]
            thewriter.writerow(info)
