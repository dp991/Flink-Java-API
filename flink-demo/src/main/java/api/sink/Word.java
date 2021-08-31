package api.sink;

import java.io.Serializable;

/**
 * @author zdp
 * @description ds
 * @email 13221018869@189.cn
 * @date 2021/5/27 14:01
 */
public class Word implements Serializable {
    private String word;
    private Integer count;

    public Word() {
    }

    public Word(String word, Integer count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "{"+word+"::"+count+"}";
    }
}
