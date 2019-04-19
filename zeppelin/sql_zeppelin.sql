
-- show top n words (sum)
select  * from
(select word,"male",sum(count) as WordFrequency 
from wordCount  group by word,gender having gender = "m" order by WordFrequency desc limit( cast("${num of top words =10}" as int))) 
union 
(select word,"female",sum(count) as WordFrequency 
from wordCount  group by word,gender having gender = "f" order by WordFrequency desc limit( cast("${num of top words =10}" as int))) 
union 
(select word,"other",sum(count) as WordFrequency 
from wordCount  group by word,gender having gender = "o" order by WordFrequency desc limit( cast("${num of top words =10}" as int))) 


-- show the changing trend of the selected word during the files' period
 select fileIndex,word,sum(count) from wordCount where word ="${word1}" or word = "${word2}" or word = "${word3 }"
        group by word,fileIndex order by fileIndex

-- show the age distribution of selected words (sum)
 select case when age < 20 then "10s"
        else 
            case when age < 30 then "20s"
            else 
                    case when age < 40 then "30s" 
                    else
                        case when age < 50 then "40s"
                        else 
                            case when age < 60 then "50s"
                            else
                                case when age < 70 then "60s"
                                end
                            end
                        end
                    end 
            end
        end as age
 ,word,sum(count) from wordCount where word ="${word}"
        group by word,age order by age


-- show the changing trend of the selected tag during the files' period
 select fileIndex,tag,sum(count) from tagCount where tag ="${tag1=#f1,#f1|#gadgetshowlive|#movie|#asot400|musicmonday|#iphone|#movie|#follow|#youtube|#google}"
 or  tag ="${tag2=#f1,#f1|#gadgetshowlive|#movie|#asot400|musicmonday|#iphone|#movie|#follow|#youtube|#google}"or  tag ="${tag3=#f1,#f1|#gadgetshowlive|#movie|#asot400|musicmonday|#iphone|#movie|#follow|#youtube|#google}"
        group by tag,fileIndex order by fileIndex




-- show top n tags (sum)
select  * from
(select tag,"male",sum(count) as TagFrequency 
from tagCount  group by tag,gender having gender = "m" order by TagFrequency desc limit( cast("${num of top tags =10}" as int))) 
union 
(select tag,"female",sum(count) as TagFrequency 
from tagCount  group by tag,gender having gender = "f" order by TagFrequency desc limit( cast("${num of top tags =10}" as int))) 
union 
(select tag,"other",sum(count) as TagFrequency 
from tagCount  group by tag,gender having gender = "o" order by TagFrequency desc limit( cast("${num of top tags =10}" as int))) 




-- show the bigram count
 select word2,sum(count) as WordFrequency from bigram where word1 ="${word}"
        group by word2 order by WordFrequency desc limit(10)



















