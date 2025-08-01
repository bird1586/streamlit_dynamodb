import streamlit as st
import boto3
from boto3.dynamodb.conditions import Key
import pandas as pd
import uuid

# ---- 密碼檢查 ----
def check_password():
    if "password_correct" not in st.session_state:
        pw = st.text_input("請輸入密碼", type="password")
        if pw:
            if pw == st.secrets["password"]:
                st.session_state["password_correct"] = True
            else:
                st.error("密碼錯誤")
                return False
        else:
            return False
    return st.session_state.get("password_correct", False)

if not check_password():
    st.stop()

st.title("DynamoDB CRUD with Streamlit")

# ---- 初始化 DynamoDB 客戶端 ----
dynamodb = boto3.resource(
    'dynamodb',
    region_name=st.secrets["aws_region"],
    aws_access_key_id=st.secrets["aws_access_key_id"],
    aws_secret_access_key=st.secrets["aws_secret_access_key"]
)
table = dynamodb.Table(st.secrets["dynamodb_table_name"])

# ---- 讀取現有資料 ----
def load_data():
    response = table.scan()
    items = response.get('Items', [])
    if not items:
        return pd.DataFrame()
    df = pd.DataFrame(items)
    return df

# 載入資料
df = load_data()

# 顯示資料
st.subheader("目前資料")
if df.empty:
    st.info("DynamoDB 無資料")
else:
    st.dataframe(df)

# ---- 新增資料 ----
st.subheader("新增資料")
new_name = st.text_input("名稱")
new_value = st.text_input("數值")

if st.button("新增"):
    if not new_name or not new_value:
        st.error("名稱與數值不可為空")
    else:
        new_id = str(uuid.uuid4())
        item = {
            'id': new_id,
            'name': new_name,
            'value': new_value
        }
        try:
            table.put_item(Item=item)
            st.success(f"新增成功，ID={new_id}")
            st.experimental_rerun()
        except Exception as e:
            st.error(f"新增失敗: {e}")

# ---- 修改或刪除資料 ----
st.subheader("修改 / 刪除資料")
if df.empty:
    st.info("無法修改或刪除，資料表為空")
else:
    selected_id = st.selectbox("選擇要修改或刪除的ID", df['id'])

    selected_record = df[df['id'] == selected_id].iloc[0]

    new_name_edit = st.text_input("名稱", value=selected_record.get('name', ''))
    new_value_edit = st.text_input("數值", value=selected_record.get('value', ''))

    col1, col2 = st.columns(2)

    with col1:
        if st.button("更新資料"):
            try:
                table.update_item(
                    Key={'id': selected_id},
                    UpdateExpression="SET #n = :name, #v = :val",
                    ExpressionAttributeNames={
                        '#n': 'name',
                        '#v': 'value',
                    },
                    ExpressionAttributeValues={
                        ':name': new_name_edit,
                        ':val': new_value_edit,
                    }
                )
                st.success("更新成功")
                st.experimental_rerun()
            except Exception as e:
                st.error(f"更新失敗: {e}")

    with col2:
        if st.button("刪除資料"):
            try:
                table.delete_item(Key={'id': selected_id})
                st.success("刪除成功")
                st.experimental_rerun()
            except Exception as e:
                st.error(f"刪除失敗: {e}")

