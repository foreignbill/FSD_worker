import pymysql
import requests


class DataManage:
    """用于管理数据库中模型，数据集，链接和权限的模块

       参数
     -------
     username:用户名,使用upload_model_to_user时为servername
     pwd:用户密码,使用upload_model_to_user时为server密码
    """

    def __init__(self, username, pwd, ):
        self.username = username
        self.pwd = pwd

    def upload_model(self, is_public, task, model_name, dataset_name, file_path, url):
        """上传一个模型
        :param is_public: 是否公开，Ture表示公开，False表示不公开
        :param task: 任务
        :param model_name: 模型名
        :param dataset_name: 数据集名
        :param file_path: 文件路径
        :param url: 服务器端上传文件的接口地址
        :return: None
        """
        _data = {
            "is_public": is_public, "task": task, "model_name": model_name,
            "dataset_name": dataset_name
        }
        str_data = str(_data).replace("'", "\"")
        data = {"file_info": str_data}
        file = {"file": open(file_path, 'rb')}
        auth = (self.username, self.pwd)
        r = requests.post(url, data=data, files=file, auth=auth)

    def upload_dataset(self, is_public, file_path, dataset_name, url):
        """
        上传一个数据集
        :param is_public: 是否公开，Ture表示公开，False表示不公开
        :param file_path:文件路径
        :param dataset_name: 数据集名称
        :param url:服务器端上传数据集的接口地址
        :return: None
        """

        _data = {
            "is_public": is_public, "dataset_name": dataset_name,
        }
        str_data = str(_data).replace("'", "\"")
        data = {"file_info": str_data}
        file = {"file": open(file_path, 'rb')}
        auth = (self.username, self.pwd)
        r = requests.post(url, data=data, files=file, auth=auth)

    def delete_model(self, model_ids, url):
        """
        :param model_ids: 要删除模型id的列表
        :param url:
        :return:
        """
        # data = {"model_ids": model_ids}
        # auth = (self.username, self.pwd)
        # r = requests.delete(url, data=data, auth=auth)
        pass

    def delete_dataset(self, dataset_ids, url):
        pass

    def get_model_info(self, url):
        auth = (self.username, self.pwd)
        r = requests.get(url, auth=auth)
        print(r.text)

    def get_dataset_info(self, url):
        auth = (self.username, self.pwd)
        r = requests.get(url, auth=auth)
        print(r.text)

    def update_model(self, model_id, url, backbone, is_public, task, model_name, dataset_name):
        _data = {
            "model_id": model_id, "backbone": backbone, "is_public": is_public, "task": task, "model_name": model_name,
            "dataset_name": dataset_name
        }
        str_data = str(_data).replace("'", "\"")
        data = {"file_info": str_data}
        auth = (self.username, self.pwd)
        r = requests.put(url, data=data, auth=auth)

    def update_dataset(self, dataset_id, url, is_public, dataset_name):
        _data = {
            "dataset_id": dataset_id, "is_public": is_public, "dataset_name": dataset_name
        }
        str_data = str(_data).replace("'", "\"")
        data = {"file_info": str_data}
        auth = (self.username, self.pwd)
        r = requests.put(url, data=data, auth=auth)

    def upload_model_to_user(self, task_uid, is_public, task, model_name, dataset_name, file_path, url):
        """上传一个模型
        :param task_uid: 任务的uid字段
        :param is_public: 是否公开，Ture表示公开，False表示不公开
        :param task: 任务
        :param model_name: 模型名
        :param dataset_name: 数据集名
        :param file_path: 文件路径
        :param url: 服务器端上传文件的接口地址
        :return: None
        """
        # TODO: foreign key check
        _data = {
            "task_uid": task_uid, "is_public": is_public, "task": task, "model_name": model_name,
            "dataset_name": dataset_name, "server_name": self.username, "pwd": self.pwd,
        }
        str_data = str(_data).replace("'", "\"")
        data = {"file_info": str_data}
        file = {"file": open(file_path, 'rb')}
        r = requests.post(url, data=data, files=file)
        return r

    def upload_export(self, task_uid, file_path, url):
        """上传一个模型
        :param task_uid: 任务的uid字段
        :param is_public: 是否公开，Ture表示公开，False表示不公开
        :param task: 任务
        :param model_name: 模型名
        :param dataset_name: 数据集名
        :param file_path: 文件路径
        :param url: 服务器端上传文件的接口地址
        :return: None
        """
        # TODO: foreign key check
        _data = {
            "task_uid": task_uid, "server_name": self.username, "pwd": self.pwd,
        }
        str_data = str(_data).replace("'", "\"")
        data = {"file_info": str_data}
        file = {"file": open(file_path, 'rb')}
        r = requests.post(url, data=data, files=file)
        return r


if __name__ == '__main__':
    # test code
    d = DataManage(
        username="client_1", pwd="123",
    )
    res = d.upload_model_to_user(
        task_uid="0134868aab7f11ecb93ea4bb6de09ef7", is_public="False", task="0134868aab7f11ecb93ea4bb6de09ef7",
        dataset_name="backend",
        model_name=".zshrc", file_path="/home/ghk/FSD_workerspace/0134868aab7f11ecb93ea4bb6de09ef7/export.zip",
        url="http://10.214.211.151:8088/api/model/upload_model_to_user"
    )
    print(res.text)
    print(res)
