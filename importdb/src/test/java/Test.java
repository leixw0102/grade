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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-12-3
 * Time: 下午5:56
 */
public class Test {
    public static void main(String[]args) throws IOException {
        URL url = new URL("http://vod01.media.ysten.com//media/new/2012/zhuanti/20120404/sd_dsj_zawd02_20120404.ts");
        System.out.println(url.getProtocol()+"\t"+url.getAuthority() + "\t" + url.getPath() +"\t");
    }
}
