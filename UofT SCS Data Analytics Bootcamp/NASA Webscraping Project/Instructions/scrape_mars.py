#!/usr/bin/env python
# coding: utf-8
from bs4 import BeautifulSoup
import requests
import pymongo
from splinter import Browser
from splinter.exceptions import ElementDoesNotExist
import pandas as pd
# In[1]:

def init_browser():
    # @NOTE: Replace the path with your actual path to the chromedriver
    executable_path = {"executable_path": "/usr/local/bin/chromedriver"}
    return Browser("chrome", **executable_path, headless=False)

def scrape():
    
    


# In[2]:


    browser=init_browser()


# In[4]:


    url = "https://mars.nasa.gov/news/?page=0&per_page=40&order=publish_date+desc%2Ccreated_at+desc&search=&category=19%2C165%2C184%2C204&blank_scope=Latest"


# In[5]:


    browser.visit(url)


# In[26]:


    html = browser.html
    soup=BeautifulSoup(html,'html.parser')


# In[9]:


    content_title = soup.find('div',class_="content_title")
    content_title


# In[15]:


    news_title = content_title.find('a').text
    


# In[16]:


    news_p = soup.find('div',class_='article_teaser_body').text
    


# In[35]:


    jpl_url = "https://www.jpl.nasa.gov/spaceimages/?search=&category=Mars"


# In[36]:


    browser.visit(jpl_url)


# In[37]:


    html = browser.html
    soup=BeautifulSoup(html,'html.parser')


# In[43]:


    article = soup.find("article",class_="carousel_item")
    footer = article.find("footer")
    a = footer.find("a")
    featured_image_url = "https://www.jpl.nasa.gov" + a["data-fancybox-href"]
    


# In[13]:


    twitter_url = 'https://twitter.com/marswxreport?lang=en'


# In[14]:


    browser.visit(twitter_url)


# In[15]:


    html=browser.html
    soup=BeautifulSoup(html,'html.parser')


# In[18]:


    twitter_div = soup.find('div',class_='js-tweet-text-container')
    


# In[20]:


    mars_weather=twitter_div.find('p').text
    


# In[4]:


    table_url = 'https://space-facts.com/mars/'


# In[6]:


    tables = pd.read_html(table_url)
    


# In[11]:


    df = tables[0]
    df.rename(columns={0:"Description", 1:"Value"},inplace=True)
    df = df.set_index("Description")



# In[13]:


    html_table = df.to_html()
    #html_table = html_table.replace('\n','')



# In[28]:


    url_list=["https://astrogeology.usgs.gov/search/map/Mars/Viking/cerberus_enhanced","https://astrogeology.usgs.gov/search/map/Mars/Viking/schiaparelli_enhanced","https://astrogeology.usgs.gov/search/map/Mars/Viking/syrtis_major_enhanced","https://astrogeology.usgs.gov/search/map/Mars/Viking/valles_marineris_enhanced"]


# In[33]:


    dict_list = []
    for url in url_list:
        browser.visit(url)
        html=browser.html
        soup=BeautifulSoup(html,'html.parser')
        link = soup.find("img",class_ = "wide-image")
        full_url="https://astrogeology.usgs.gov"+link["src"]
        name = soup.find("h2",class_="title").text
        name_url_dict = {"title":name,
                        "img_url":full_url}
        dict_list.append(name_url_dict)

    
    data = {"news_title":news_title,
            "news_p":news_p,
            "featured_image_url":featured_image_url,
            "mars_weather":mars_weather,
            "data_table":html_table,
            "hemisphere_dict_list":dict_list}
    browser.quit()
    return data

scrape()



