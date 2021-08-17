
import os
import  time
import requests
from logg import *
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager







class AdvanceImageDownloader():

    def __init__(self,search,count,url,img_links,keywords):

        log = Logger("*** image_scrapper_logger ***")
        log.logging_info("-----------------  Image Scrapper Log Started --------------------")
        logger = log.image_scrapper()

        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--ignore-certificate-errors')
        self.options.add_argument('--user-agent="Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/16D57"')

        # driver = webdriver.PhantomJS("./phantomjs")
        self.driver = webdriver.Chrome(ChromeDriverManager().install())
        self.img_links = img_links
        self.keywords = keywords
        self.search = search
        self.count = count
        self.url = url



    def get_image_links(self,keyword):
        flag = True
        i = 1
        stop_1 = False
        more_video = True
        url = 'https://www.bing.com/images/search?q=' + keyword + '&form=HDRSC2&first=1&tsc=ImageBasicHover'
        self.driver.get(url)
        while flag:
            # if (len(img_links)+i*4)==count:
            #     stop_1 = True
            try:
                print('Done.........................')
                source1 = self.driver.find_element_by_xpath('//*')
                source = source1.get_attribute('innerHTML')
                if source.__contains__('class="expandButton txtaft disabled"'):
                    if more_video:
                        self.driver.execute_script("""document.querySelector('[class="btn_seemore cbtn mBtn"]').click()""")
                        more_video = False
                    else:
                        links = source.split('src="')
                        for i in links[1:]:
                            img = i.split('"')[0]

                            if img.__contains__('https://th.bing.com/th/id/OIP.'):
                                self.img_links.append(img)

                        flag = False
                elif stop_1:
                    links = source.split('src="')
                    for i in links[1:]:
                        img = i.split('"')[0]
                        if img.__contains__('https://th.bing.com/th/id/OIP.'):
                            self.img_links.append(img)
                    flag = False
                    self.driver.close()

                else:
                    source1 = self.driver.find_element_by_xpath('//*')
                    source = source1.get_attribute('innerHTML')
                    if source.__contains__(f'data-row="{i - 1}"'):


                        if (len(self.img_links) + (i-1) * 4) >= self.count:
                            stop_1 = True
                        else:
                            scr1 = self.driver.find_element_by_xpath('//*[@id="mmComponent_images_2"]/ul[%s]' % i)
                            self.driver.execute_script("arguments[0].scrollIntoView();", scr1)
                            i = i + 1

                    elif source.__contains__(f'data-idx="{i}"'):
                        if (len(self.img_links) + i * 5) >= self.count:
                            stop_1 = True
                        else:
                            scr1 = self.driver.find_element_by_xpath('//*[@class="dgControl_list"]/li[%s]' % i)
                            self.driver.execute_script("arguments[0].scrollIntoView();", scr1)
                            i = i + 1


            except:
                pass
        return stop_1





    def google_keywords(self):
        self.keywords.append(self.search)
        url = 'https://www.google.com/search?q=' + self.search + '&sxsrf=ALeKk01TEbIS5DZCbwMaMBb2TR3bStB0bQ:1627814853282&source=lnms&tbm=isch&sa=X&ved=2ahUKEwjt3dvw0o_yAhXGzjgGHYLoChAQ_AUoAnoECAIQBA&biw=1008&bih=827'
        self.driver.get(url)

        while True:
            try:
                source1 = self.driver.find_element_by_xpath('//*[@id="i6"]/div[3]')
                source = source1.get_attribute('innerHTML')
                # print(source)

                if source.__contains__('style="display: none;"'):
                    source1 = self.driver.find_element_by_xpath('//*')
                    source = source1.get_attribute('innerHTML')
                    key_list = source.split('span class="hIOe2">')
                    for i in key_list[1:]:
                        if (i.split('<')[0]).__contains__(f'{self.search}'):
                            self.keywords.append(i.split('<')[0])
                        else:
                            key = i.split('<')[0]
                            self.keywords.append(key + f' {self.search}')
    #                 print(keywords)
    #                 print(len(keywords))
                    break
                else:
                    self.driver.execute_script("""document.querySelector('[class="cEW58 khjlM"]').click()""")
                    time.sleep(1)

            except Exception as e:
               break


    def bing_keywords(self):
        url = 'https://www.bing.com/images/search?q=' + self.search + '&form=HDRSC2&first=1&tsc=ImageBasicHover'
        self.driver.get(url)
        while True:
            try:
                source1 = self.driver.find_element_by_xpath('//*')
                source = source1.get_attribute('innerHTML')
                if source.__contains__('class="nav_container nav_right dis"'):
                    key_list = source.split('title="Search for: ')
                    for i in key_list[1:]:
                        if (i.split('"')[0]).__contains__(f'{self.search}'):
                            self.keywords.append(i.split('"')[0])
        #             print(keywords)
        #             print(len(keywords))
                    break
                else:
                    # driver.execute_script("""document.querySelector('[class="nav_container nav_right"]').click()"]').click()""")
                    self.driver.execute_script("""document.querySelector('[class="nav_container nav_right"]').click()""")
            except Exception as e:
                break

    def get_image_link(self):
        stop=True
        print("length of keywords : ",len(self.keywords))
        i = 1
        for j in self.keywords:
            if stop:
                stop = not self.get_image_links(j)
            else:
                break
            i+=1
            print(i)


    def img_download_content(self):
        l_img = []
        for image in self.img_links[:self.count]:
            img = requests.get(image)
            l_img.append(img.content)
            # con.insert_into_image_table(str(img))
        return l_img


    def image_original_link_list(self):
        i = 1
        if not os.path.isdir(f'All_img/{self.search}'):
            os.makedirs(f'All_img/{self.search}')
        for im in self.img_download_content():
            with open(f'All_img/{self.search}/{self.search}{i}.png','wb') as f:
                f.write(im)
                f.close()
                i+=1









def Download(obj):

        print("done")
        obj.google_keywords()
        print('done')
        obj.bing_keywords()
        print('done')
        obj.get_image_link()
        obj.image_original_link_list()
        print('done images')







if __name__ == '__main__':

    search = input('Enter Image Name: ').capitalize()
    count = int(input('Total Imagees: '))
    url = 'https://www.bing.com/images/search?q=' + search + '&form=HDRSC2&first=1&tsc=ImageBasicHover'
    object = AdvanceImageDownloader(search, count,'https://www.bing.com/images/search?q=' + search + '&form=HDRSC2&first=1&tsc=ImageBasicHover')
    image = Download(object)
    Download(object)









