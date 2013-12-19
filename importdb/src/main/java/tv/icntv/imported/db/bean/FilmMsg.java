/* Copyright 2013 Future TV, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */

package tv.icntv.imported.db.bean;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-20

 * Time: 下午3:05
 */
public class FilmMsg {
    private static final String filedSplit="`";
    private long id;//节目id
    private long time;
    private String url;
    private String name;  //名称
    private String tag;         //标签
    private String category;          //类型
    private String year;
    private String director;
    private String actors;
    private String zone;
    private String ppvCode;
    private String writer;//编剧
    private long programId;//节目集id

    public long getProgramId() {
        return programId;
    }

    public void setProgramId(long programId) {
        this.programId = programId;
    }

    public String getWriter() {
        return writer;
    }

    public void setWriter(String writer) {
        this.writer = writer;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    private FilmMsg() {
    }

    @Override
    public FilmMsg clone()  {
        try {
            return (FilmMsg) super.clone();    //To change body of overridden methods use File | Settings | File Templates.
        } catch (CloneNotSupportedException e) {
            return new FilmMsg();
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getDirector() {
        return director;
    }

    public void setDirector(String director) {
        this.director = director;
    }

    public String getActors() {
        return actors;
    }

    public void setActors(String actors) {
        this.actors = actors;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public String getPpvCode() {
        return ppvCode;
    }

    public FilmMsg(long id) {
        this.id = id;
    }

    public void setPpvCode(String ppvCode) {
        this.ppvCode = ppvCode;
    }

    @Override
    public String toString() {
       StringBuffer sb=new StringBuffer();
        sb.append(this.getId()).append(filedSplit).append(this.getUrl()).append(filedSplit).append(this.getTime()*60).append(filedSplit)
        .append(this.getName()).append(filedSplit).append(this.getTag()).append(filedSplit).append(this.getCategory()).append(filedSplit)
        .append(this.getYear()).append(filedSplit).append(this.getDirector()).append(filedSplit).append(this.getActors()).append(filedSplit)
        .append(this.getZone()).append(filedSplit).append(this.getPpvCode()).append(filedSplit).append(this.getWriter()).append(filedSplit)
        .append(this.getProgramId());
        return sb.toString();
    }

    public static class FileMsgBuilder{
        private long id;//节目id
        private long time;
        private String url;
        private String name;  //名称
        private String tag;         //标签
        private String category;          //类型
        private String year;
        private String director;
        private String actors;
        private String zone;
        private String ppvCode;
        private String writer;
        private long programId;//节目集id

        public long getProgramId() {
            return programId;
        }

        public FileMsgBuilder setProgramId(long programId) {
            this.programId = programId;
            return this;
        }

        public String getWriter() {
            return writer;
        }

        public FileMsgBuilder setWriter(String writer) {
            this.writer = writer;
            return this;
        }

        public String getName() {
            return name;
        }

        public FileMsgBuilder setName(String name) {
            this.name = name;
            return this;
        }

        public String getTag() {
            return tag;
        }

        public FileMsgBuilder setTag(String tag) {
            this.tag = tag;
            return this;
        }

        public String getCategory() {
            return category;
        }

        public FileMsgBuilder setCategory(String category) {
            this.category = category;
            return this;
        }

        public String getYear() {
            return year;
        }

        public FileMsgBuilder setYear(String year) {
            this.year = year;
            return this;
        }

        public String getActors() {
            return actors;
        }

        public FileMsgBuilder setActors(String actors) {
            this.actors = actors;
            return this;
        }

        public String getZone() {
            return zone;
        }

        public FileMsgBuilder setZone(String zone) {
            this.zone = zone;
            return this;
        }

        public String getDirector() {
            return director;
        }

        public FileMsgBuilder setDirector(String director) {
            this.director = director;
            return this;
        }

        public String getPpvCode() {
            return ppvCode;
        }

        public FileMsgBuilder setPpvCode(String ppvCode) {
            this.ppvCode = ppvCode;
            return this;
        }

        public long getId() {
            return id;
        }

        public FileMsgBuilder setId(long id) {
            this.id = id;
            return this;
        }

        public long getTime() {
            return time;
        }

        public FileMsgBuilder setTime(long time) {
            this.time = time;
            return this;
        }

        public String getUrl() {
            return url;
        }

        public FileMsgBuilder setUrl(String url) {
            this.url = url;
            return this;
        }
        public FilmMsg builder(){
            return new FilmMsg(this.getId(),this.getTime(),this.getUrl(),this.getName()
            ,this.getTag(),this.getCategory(),this.getYear(),this.getDirector(),this.getActors()
            ,this.getZone(),this.getPpvCode(),this.getWriter(),this.getProgramId());
        }
    }

    private FilmMsg(long id, long time, String url, String name, String tag, String category, String year, String director, String actors, String zone, String ppvCode,String writer,long programId) {
        this.id = id;
        this.time = time;
        this.url = url;
        this.name = name;
        this.tag = tag;
        this.category = category;
        this.year = year;
        this.director = director;
        this.actors = actors;
        this.zone = zone;
        this.ppvCode = ppvCode;
        this.writer=writer;
        this.programId=programId;
    }
}
