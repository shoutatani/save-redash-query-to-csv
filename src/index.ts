import axios, { AxiosResponse } from "axios";
import fs from "graceful-fs";
import writeJsonFile from "write-json-file";

type QueriesResultType = {
  is_archived: boolean;
  schedule: null | string;
  retrieved_at: Date;
  updated_at: Date;
  query: string;
  id: number;
  name: string;
  data_source_id: number;
};

type QueriesResponse = {
  count: number;
  page: number;
  page_size: number;
  results: QueriesResultType[];
};

const axiosInstance = axios.create({
  baseURL: "http://<redash-hosted-url>/api",
  headers: { Authorization: "Key <xxx>" },
});

const sleep = (time: number | undefined = 1000): Promise<void> => {
  return new Promise((resolve, _reject) => {
    setTimeout(() => {
      resolve();
    }, time);
  });
};

const main = async () => {
  const firstResponse = await retrieveQuery();
  console.log("start get queries page = ", firstResponse.data.page);
  const maxPage = Math.ceil(
    firstResponse.data.count / firstResponse.data.page_size
  );
  const results: QueriesResultType[][] = [];
  results.push(firstResponse.data.results);
  for (let page = 2; page <= maxPage; page++) {
    await sleep(2000);
    console.log("start get queries page = ", page);
    const res = await retrieveQuery(page);
    results.push(res.data.results);
  }

  const allQueriesFilePath = `data/json/all_queries_${Date.now()}.json`;
  await writeJsonFile(allQueriesFilePath, results.flat());
  convertJsonQueriesToCsvFiles(allQueriesFilePath, "formatted_all_Queries");
};

const retrieveQuery = async (
  page: number = 1
): Promise<AxiosResponse<QueriesResponse>> => {
  const url = "/queries";
  const response = axiosInstance.get<QueriesResponse>(url, {
    params: {
      page,
    },
  });
  return response;
};

const convertJsonQueriesToCsvFiles = (
  filePath: string,
  formattedFileName: string
) => {
  const readFilePath = `./${filePath}`;
  const jsonObject = JSON.parse(fs.readFileSync(readFilePath, "utf8"));
  const header =
    '"id","name","query","last_modified_by_id","latest_query_data_id","schedule","is_archived","retrieved_at","updated_at","user_name","is_draft","description","created_at","data_source_id"';

  const size: number = jsonObject.length;
  let result = "\n";
  for (let i = 0; i < size; i++) {
    const target = jsonObject[i];
    result +=
      `"${target.id}","${target.name}","${target.query.replace(/\"/g, "'")}","${
        target.last_modified_by_id
      }","${target.latest_query_data_id}","${target.schedule}","${
        target.is_archived
      }","${target.retrieved_at}","${target.updated_at}","${
        target.user.name
      }","${target.is_draft}","${target.description?.replace(/\"/g, "'")}","${
        target.created_at
      }","${target.data_source_id}"` + "\n";
  }
  fs.writeFileSync(
    `./data/csv/${formattedFileName}_${Date.now()}.csv`,
    header + result
  );
};

main();
