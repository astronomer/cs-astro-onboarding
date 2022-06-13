begin;

drop table if exists public.top_ten_buyers_by_quantity;
create table public.top_ten_buyers_by_quantity as (
  with q as (
    select
      buyerid,
      sum(qtysold) as total_quantity
    from sales
    group by buyerid
    order by total_quantity
    desc
    limit 10
  )

  select
    firstname,
    lastname,
    total_quantity
  from q, users
  where q.buyerid = userid
);

end;