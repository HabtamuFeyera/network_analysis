CREATE TABLE "channels" (
  "channel_id" integer,
  "user_count" integer,
  "user_id" integer,
  "created_at" timestamp
);

CREATE TABLE "users" (
  "user_id" integer PRIMARY KEY,
  "username" varchar,
  "created_at" timestamp
);

CREATE TABLE "msg_content" (
  "sender" varchar PRIMARY KEY,
  "msg_type" varchar,
  "msg_content" text,
  "user_id" integer,
  "created_at" timestamp
);

COMMENT ON COLUMN "msg_content"."msg_content" IS 'Content of the post';

ALTER TABLE "channels" ADD FOREIGN KEY ("user_id") REFERENCES "users" ("user_id");

ALTER TABLE "msg_content" ADD FOREIGN KEY ("user_id") REFERENCES "users" ("user_id");

ALTER TABLE "msg_content" ADD FOREIGN KEY ("user_id") REFERENCES "channels" ("user_id");
