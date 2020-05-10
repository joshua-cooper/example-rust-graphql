use juniper::http::graphiql::graphiql_source;
use juniper::http::GraphQLRequest;
use juniper::RootNode;
use std::convert::Infallible;
use std::sync::Arc;
use tokio_postgres::{Client, NoTls};
use warp::Filter;

#[derive(juniper::GraphQLObject)]
struct Customer {
    id: String,
    name: String,
    age: i32,
    email: String,
    address: String,
}

struct QueryRoot;
struct MutationRoot;

#[juniper::graphql_object(Context = Context)]
impl QueryRoot {
    async fn customer(ctx: &Context, id: String) -> juniper::FieldResult<Customer> {
        let uuid = uuid::Uuid::parse_str(&id)?;
        let row = ctx
            .client
            .query_one(
                "SELECT name, age, email, address FROM customers WHERE id = $1",
                &[&uuid],
            )
            .await?;
        let customer = Customer {
            id,
            name: row.try_get(0)?,
            age: row.try_get(1)?,
            email: row.try_get(2)?,
            address: row.try_get(3)?,
        };
        Ok(customer)
    }

    async fn customers(ctx: &Context) -> juniper::FieldResult<Vec<Customer>> {
        let rows = ctx
            .client
            .query("SELECT id, name, age, email, address FROM customers", &[])
            .await?;
        let mut customers = Vec::new();
        for row in rows {
            let id: uuid::Uuid = row.try_get(0)?;
            let customer = Customer {
                id: id.to_string(),
                name: row.try_get(1)?,
                age: row.try_get(2)?,
                email: row.try_get(3)?,
                address: row.try_get(4)?,
            };
            customers.push(customer);
        }
        Ok(customers)
    }
}

#[juniper::graphql_object(Context = Context)]
impl MutationRoot {
    async fn register_customer(
        ctx: &Context,
        name: String,
        age: i32,
        email: String,
        address: String,
    ) -> juniper::FieldResult<Customer> {
        let id = uuid::Uuid::new_v4();
        let email = email.to_lowercase();
        ctx.client
            .execute(
                "INSERT INTO customers (id, name, age, email, address) VALUES ($1, $2, $3, $4, $5)",
                &[&id, &name, &age, &email, &address],
            )
            .await?;
        Ok(Customer {
            id: id.to_string(),
            name,
            age,
            email,
            address,
        })
    }

    async fn update_customer_email(
        ctx: &Context,
        id: String,
        email: String,
    ) -> juniper::FieldResult<String> {
        let uuid = uuid::Uuid::parse_str(&id)?;
        let email = email.to_lowercase();
        let n = ctx
            .client
            .execute(
                "UPDATE customers SET email = $1 WHERE id = $2",
                &[&email, &uuid],
            )
            .await?;
        if n == 0 {
            return Err("User does not exist".into());
        }
        Ok(email)
    }

    async fn delete_customer(ctx: &Context, id: String) -> juniper::FieldResult<bool> {
        let uuid = uuid::Uuid::parse_str(&id)?;
        let n = ctx
            .client
            .execute("DELETE FROM customers WHERE id = $1", &[&uuid])
            .await?;
        if n == 0 {
            return Err("User does not exist".into());
        }
        Ok(true)
    }
}

type Schema = RootNode<'static, QueryRoot, MutationRoot>;

struct Context {
    client: Client,
}

impl juniper::Context for Context {}

async fn graphql(
    schema: Arc<Schema>,
    ctx: Arc<Context>,
    req: GraphQLRequest,
) -> Result<impl warp::Reply, Infallible> {
    let res = req.execute_async(&schema, &ctx).await;
    let json = serde_json::to_string(&res).expect("Invalid JSON response");
    Ok(json)
}

#[tokio::main]
async fn main() {
    let (client, connection) = tokio_postgres::connect("host=localhost user=postgres", NoTls)
        .await
        .unwrap();

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client
        .execute(
            "CREATE TABLE IF NOT EXISTS customers(
            id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            age INT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            address TEXT NOT NULL
        )",
            &[],
        )
        .await
        .expect("Could not create table");

    let schema = Arc::new(Schema::new(QueryRoot, MutationRoot));
    // Create a warp filter for the schema
    let schema = warp::any().map(move || Arc::clone(&schema));

    let ctx = Arc::new(Context { client });
    // Create a warp filter for the context
    let ctx = warp::any().map(move || Arc::clone(&ctx));

    let graphql_route = warp::post()
        .and(warp::path!("graphql"))
        .and(schema.clone())
        .and(ctx.clone())
        .and(warp::body::json())
        .and_then(graphql);

    let graphiql_route = warp::get()
        .and(warp::path!("graphiql"))
        .map(|| warp::reply::html(graphiql_source("graphql")));

    let routes = graphql_route.or(graphiql_route);

    warp::serve(routes).run(([127, 0, 0, 1], 8000)).await;
}
