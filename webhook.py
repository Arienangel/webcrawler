import aiohttp
import discord


async def send_discord(webhook_url, author=None, title=None, time=None, content=None, post_url=None, **kwargs):
    embed = discord.Embed(title=title, description=content)
    if author: embed.set_author(name=author)
    if post_url: embed.add_field(name='網址', value=post_url, inline=False)
    if time: embed.add_field(name='時間', value=f'<t:{time}>', inline=False)
    async with aiohttp.ClientSession() as session:
        webhook = discord.Webhook.from_url(webhook_url, session=session)
        await webhook.send(embed=embed)
