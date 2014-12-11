




      avg_tf = 0
      cnt    = 0
      for (_ , noun) in all_noun_list:
        tmp = count_noun(noun , title , data)
        if tmp > 0:
          cnt += 1
          avg_tf += tmp
      avg_tf /= (1 if cnt == 0 else cnt)

      # いままでに得られている 名詞のリストも考える
      # avg_tf が0 って事は既存の名詞を含まない様な文書ということなので
      if avg_tf != 0:
        for (num , (n_id,noun)) in enumerate(all_noun_list): #enumerate(self.db.select(["n_id","noun"] , "nmapper")):
          freq  = (count_noun(noun , title , data)) / avg_tf 
          if freq == 0 : 
            continue
          else:
            self.registfreq(n_id , r_id , freq)
            self.registplace(n_id , r_id , self.getoccurrence(noun,data))
            if ((num % 400) == 0):self.db.commit()
        self.db.commit()

      # avg(s<-d ,  tf(s,d)) を求める。これが分母となる
      avg_tf = reduce(lambda r,x: r + count_noun(x,title,data) , noun_list , 0)/float(len_nlist)

      # data 自身が含んでいる名詞について考える
      for noun in noun_list:
        # ストップワードやゴミについては索引を作らない
        if self.stop_word(noun): continue
        (n_id , new) = self.registnoun(noun.encode("utf-8"))
        if new : all_noun_list.append((n_id,noun))
        freq  = (count_noun(noun , title , data)) / avg_tf 
        if freq == 0 : 
          continue
        else:
          self.registfreq(n_id , r_id , freq)
          self.registplace(n_id , r_id , self.getoccurrence(noun,data))
      self.indexed(r_id)
      self.db.commit()


