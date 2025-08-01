import streamlit as st
import boto3
import pandas as pd
import uuid
from copy import deepcopy

# ----- 密碼保護 -----
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

st.title("DynamoDB 資料表 CRUD (st.data_editor)")

# ----- DynamoDB 初始化 -----
dynamodb = boto3.resource(
    'dynamodb',
    region_name=st.secrets["aws_region"],
    aws_access_key_id=st.secrets["aws_access_key_id"],
    aws_secret_access_key=st.secrets["aws_secret_access_key"]
)
table_name = st.secrets["dynamodb_table_name"]
table = dynamodb.Table(table_name)

# ----- 讀取資料 -----
@st.cache_data(ttl=60, show_spinner=False)
def load_data():
    items = []
    last_evaluated_key = None
    while True:
        if last_evaluated_key:
            resp = table.scan(ExclusiveStartKey=last_evaluated_key)
        else:
            resp = table.scan()
        items.extend(resp.get('Items', []))
        last_evaluated_key = resp.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break
    if not items:
        return pd.DataFrame()
    df = pd.DataFrame(items)
    df['id'] = df['id'].astype(str)  # 確保 id 是 string
    return df

# ----- DynamoDB CRUD 操作 -----
def put_item(item):
    try:
        table.put_item(Item=item)
        return True, None
    except Exception as e:
        return False, str(e)

def update_item(item):
    try:
        key = {'id': item['id']}
        update_expr = "SET "
        expr_attr_vals = {}
        expr_attr_names = {}
        updates = []
        for k, v in item.items():
            if k == 'id':
                continue
            updates.append(f"#{k} = :{k}")
            expr_attr_names[f"#{k}"] = k
            expr_attr_vals[f":{k}"] = v
        update_expr += ", ".join(updates)
        table.update_item(
            Key=key,
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_attr_names,
            ExpressionAttributeValues=expr_attr_vals
        )
        return True, None
    except Exception as e:
        return False, str(e)

def delete_item(item_id):
    try:
        table.delete_item(Key={'id': item_id})
        return True, None
    except Exception as e:
        return False, str(e)

# ----- DataFrame 顯示輔助 -----
def add_index_col(df):
    df = df.copy()
    df.insert(0, "序號", range(1, len(df) + 1))
    return df

def fill_missing_ids(df):
    def gen_id(val):
        if val is None or (isinstance(val, float) and pd.isna(val)) or str(val).strip() == "":
            return str(uuid.uuid4())
        else:
            return val
    df['id'] = df['id'].apply(gen_id)
    return df

# ----- 比對差異 -----
def diff_dfs(old_df, new_df):
    old_ids = set(old_df['id'].astype(str))
    new_ids = set(new_df['id'].astype(str))

    added_ids = new_ids - old_ids
    deleted_ids = old_ids - new_ids
    possible_modified_ids = new_ids.intersection(old_ids)

    added_rows = new_df[new_df['id'].isin(added_ids)]
    deleted_rows = old_df[old_df['id'].isin(deleted_ids)]

    modified_rows = []
    for idx, row in new_df[new_df['id'].isin(possible_modified_ids)].iterrows():
        oid = row['id']
        old_row = old_df[old_df['id'] == oid].iloc[0]
        if row.astype(str).equals(old_row.astype(str)) is False:
            modified_rows.append(row)
    modified_rows_df = pd.DataFrame(modified_rows)

    return added_rows, deleted_rows, modified_rows_df

# ----- 主流程 -----
if "refresh_data" not in st.session_state:
    st.session_state.refresh_data = True

col1, _ = st.columns([1, 8])
with col1:
    if st.button("刷新表格內容"):
        st.cache_data.clear()
        st.session_state.refresh_data = True

if st.session_state.refresh_data:
    df_original = load_data()
    st.session_state.df_original = df_original
    st.session_state.df_edit = deepcopy(df_original)
    st.session_state.refresh_data = False
else:
    df_original = st.session_state.get("df_original", pd.DataFrame())
    st.session_state.df_edit = st.session_state.get("df_edit", deepcopy(df_original))

if df_original.empty:
    st.info("目前資料表空白。")

st.write("編輯表格後，請點擊『提交變更』同步資料。")
st.text("可直接修改資料。新增可在最下方空白列操作。刪除資料請清空該行所有欄位。")

df_display = add_index_col(st.session_state.df_edit)

column_config = {
    "序號": st.column_config.Column(read_only=True),
    "id": st.column_config.Column(header="UUID (ID)", read_only=True)
}

edited_df = st.data_editor(
    df_display,
    column_config=column_config,
    num_rows="dynamic",
    use_container_width=True,
)

# 儲存編輯中 dataframe（不含序號欄）
st.session_state.df_edit = edited_df.drop(columns=['序號'], errors='ignore')

if st.button("提交變更"):
    df_to_process = st.session_state.df_edit.copy()
    df_to_process = fill_missing_ids(df_to_process)  # 填補新列缺少id

    added_rows, deleted_rows, modified_rows = diff_dfs(df_original, df_to_process)

    with st.spinner("同步變更至 DynamoDB..."):
        errors = []

        for _, row in deleted_rows.iterrows():
            ok, err = delete_item(row['id'])
            if not ok:
                errors.append(f"刪除ID={row['id']} 錯誤: {err}")

        for _, row in added_rows.iterrows():
            item = row.to_dict()
            ok, err = put_item(item)
            if not ok:
                errors.append(f"新增ID={item['id']} 錯誤: {err}")

        for _, row in modified_rows.iterrows():
            item = row.to_dict()
            ok, err = update_item(item)
            if not ok:
                errors.append(f"更新ID={item['id']} 錯誤: {err}")

    if errors:
        st.error("部分操作失敗:\n" + "\n".join(errors))
    else:
        st.success("資料庫同步成功！")
        st.cache_data.clear()
        st.session_state.refresh_data = True
        st.experimental_rerun()
